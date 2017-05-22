package queue_reader

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/centrifugal/gocent"
	"github.com/garyburd/redigo/redis"
	expinf "github.com/rpoletaev/exportinfo"
	"github.com/weekface/mgorus"
	mgo "gopkg.in/mgo.v2"
	//"path/filepath"
	// "bufio"
	"bytes"
	"errors"
	"io"
	"sync"
)

var serviceURLMu sync.Mutex
var verMap map[string]string
var errEmptyVerString = errors.New("Не удалось найти строку с версией")

const linkTemplate = "http://%s%s:%s/load"
const errDataSendTemplate = "файл %s | %s | %s | %v"
const schemeVerString = "schemeVersion"
const lenVerOffset = 50

func init() {
	verMap = make(map[string]string, 0)
}

const MaxFileSize = 16793600

type service struct {
	*log.Logger
	*Config
	redisPool *redis.Pool
	redisConn redis.Conn
	mongo     *mgo.Session
	wsClient  *gocent.Client
	runningMu sync.RWMutex
	running   bool
	done      chan bool
	flist     chan string
}

func (svc *service) Running() bool {
	svc.runningMu.RLock()
	defer svc.runningMu.RUnlock()
	return svc.running
}

func (svc *service) SetRunning(val bool) {
	svc.runningMu.Lock()
	defer svc.runningMu.Unlock()
	svc.running = val
}

// GetService Возвращает настроенный из конфига сервис
func GetService(cnf *Config) (*service, error) {
	logHook, err := mgorus.NewHooker(cnf.LogHook.DBHost, cnf.LogHook.DBName, cnf.LogHook.System)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	svc := &service{}
	svc.Config = cnf

	svc.Logger = log.New()
	svc.Hooks.Add(logHook)
	log.SetLevel(log.ErrorLevel)

	svc.redisPool = newRedisPool(cnf.RedisAddress, cnf.RedisPassword)

	svc.setupMongo()
	svc.mongoExec("processedFiles", func(c *mgo.Collection) error {
		_, err := c.RemoveAll(nil)
		return err
	})

	go svc.RunRedisSubscribe()
	svc.wsClient = gocent.NewClient("http://"+svc.WSConfig.Host+":"+svc.WSConfig.Port, svc.WSConfig.Secret, 5*time.Second)
	return svc, nil
}

func (svc *service) log() *log.Entry {
	return svc.Logger.WithFields(log.Fields{
		"system": svc.LogHook.System,
		"host":   svc.LogHook.Host,
	})
}

// Посылает сигнал об остановке всем запущенным процессам обработки
func (svc *service) Stop() {
	if !svc.Running() {
		fmt.Println("Сервис уже остановлен")
		return
	}

	println("Остановка сервиса")
	svc.done <- true
	close(svc.done)
	svc.SetRunning(false)
}

// запустить обработку файлов из очереди загрузки
func (svc *service) run(fileGetterFunc func() (string, error)) {
	if svc.Running() {
		println("Сервис уже запущен. Выходим")
		return
	}

	svc.SetRunning(true)
	svc.WsNotify()

	var wg sync.WaitGroup
	wg.Add(svc.RoutineCount)

	svc.done = make(chan bool, 1)
	svc.flist = make(chan string, svc.RoutineCount)
	rc, err := redis.Dial("tcp", svc.RedisAddress, redis.DialDatabase(0), redis.DialPassword(svc.RedisPassword))
	if err != nil {
		panic(err)
	}

	svc.redisConn = rc

	// svc.mongoExec("processedFiles", func(c *mgo.Collection) error {
	// 	_, err := c.RemoveAll(nil)
	// 	return err
	// })

	go func() {

		for {
			select {
			case <-svc.done:
				close(svc.flist)
				return
			default:
				queueList, err := fileGetterFunc()
				if err != nil {
					svc.Stop()
					continue
				}

				for _, p := range strings.Split(queueList, ",") {
					svc.flist <- p
				}
			}
		}
	}()

	go func() {
		for i := 0; i < svc.RoutineCount; i++ {
			go func(routineNum int) {
				svc.processFile(routineNum, svc.flist)
				println("Выходим из процесса #", routineNum)
				wg.Done()
			}(i)
		}
	}()

	wg.Wait()
	debug.FreeOSMemory()
	svc.writeResultMessage()
	svc.redisConn.Close()
	svc.SetRunning(false)
	svc.log().Infoln("Обработка завершена")
}

func (svc *service) ProcessQueue() {
	svc.run(svc.fileListQueue)
}

func (svc *service) ProcessErrors() {
	svc.run(svc.fileListFromErrors)
}

// type ProccessedPath struct {
// 	Path string `bson:"path"`
// }

// var pathStruct ProccessedPath

// Обрабатываем файл:
// Получаем из источника посредством getFileFunc(), например из очереди сервиса загрузки файлов
// или из очереди ошибок для попытки поправить и сохранить в бд
// на каждом этапе обработки при возникновении ошибки пишем ошибку в специальную очередь в MongoDB
// указывая  тип ошибки, соответствующий этапу обработки. После чего выходим из функции.
// Если ни на одном из этапов обработки ошибок не возникло, то пытаемся удалить файл с диска.
// В случае неудачи так же запишем сообщение в очередь ошибок с типом ErrorRemove
func (svc *service) processFile(routineNum int, paths <-chan string) {
	cli := GetClient(time.Duration(svc.ClientTimeOut) * time.Second)
	for path := range paths {
		if path == "" {
			continue
		}

		//===================================
		//println(path)
		//===================================

		xmlBts, err := ioutil.ReadFile(path)
		if len(xmlBts) > MaxFileSize {
			svc.storeFileProcessError(ErrorBigFileSize, path, fmt.Errorf("Размер файла: %d", len(xmlBts)))
		}

		if err != nil {
			svc.storeFileProcessError(ErrorReadFile, path, err)
			fmt.Printf("Routine %d: не удалось прочесть файл: %v\n", routineNum, err)
			continue
		}

		buf := bytes.NewBuffer(xmlBts)
		verStr, err := getVersionString(buf)
		if err != nil {
			fmt.Println("Error version string ", err.Error(), " ", path)
			svc.storeFileProcessError(ErrorExportInfo, path, err)
			continue
		}

		ei, err := expinf.GetExportInfo(verStr) //string(xmlBts[0 : len(xmlBts)/3])
		if err != nil {
			svc.storeFileProcessError(ErrorExportInfo, path, err)
			continue
		}

		url := svc.GetServiceURL(ei.Version)
		err = cli.SendData(url, xmlBts)
		xmlBts = nil

		if err != nil {
			sendErr := fmt.Errorf(errDataSendTemplate, ei.Title, ei.Version, path, err)
			fmt.Println("SEND ERR ", sendErr)

			//printKnownProblem(*ei)
			svc.storeFileProcessError(ErrorSend, path, sendErr)

			continue
		}

		if err := os.Remove(path); err != nil {
			svc.storeFileProcessError(ErrorRemove, path, err)
		}
	}
}

func printKnownProblem(ei expinf.ExportInfo) {
	if ei.Title == "fcsContractSign" && ei.Version == "1.0" {
		println("known problem")
	}
}

// Получаем путь к файлу из очереди сервиса загрузки
func (svc *service) fileListQueue() (string, error) {
	return redis.String(svc.redisConn.Do("SPOP", "FileQueue"))
}

// Получаем путь к файлу из очереди ошибок
func (svc *service) fileListFromErrors() (string, error) {
	pe := []ProcessError{}
	err := svc.mongoExec(svc.ErrorCollection,
		func(col *mgo.Collection) error {
			return col.Find(nil).All(&pe)
		})

	if err != nil {
		return "", err
	}

	//TODO: Сделать удаление обработанных
	// svc.mongoExec(svc.ErrorCollection, func(col *mgo.Collection) error {
	// 	return col.RemoveId(pe.ID)
	// })
	fileList := make([]string, len(pe), len(pe))
	for i, p := range pe {
		fileList[i] = p.FilePath
	}
	return strings.Join(fileList, ","), nil
}

// GetServiceURL принимает версию данных и формирует url для сервиса
func (svc service) GetServiceURL(version string) string {
	serviceURLMu.Lock()
	defer serviceURLMu.Unlock()

	if _, ok := verMap[version]; !ok {

		switch version {
		case "1.0":
			verMap[version] = "0.9.2"
		case "4.2":
			verMap[version] = "4.4"
		case "4.3":
			verMap[version] = "4.4"
		case "4.3.100":
			verMap[version] = "4.4"
		default:
			verMap[version] = version
		}
	}

	url := fmt.Sprintf(linkTemplate, svc.ServicePreffix, verMap[version], svc.ServicePort)
	if svc.NeedLogingURL {
		svc.log().Info(url)
	}
	return url
}

func (svc *service) RunRedisSubscribe() {
	time.Sleep(30 * time.Second)
	c := svc.redisPool.Get()

	psc := redis.PubSubConn{Conn: c}
	svc.Infoln("Подписываемся на очередь")
	defer func() {
		psc.Close()
		svc.log().Infoln("Отписываемся от событий очереди")
	}()

	if err := psc.Subscribe("ProccessResult"); err != nil {
		svc.Error(err)
		return
	}

	for {
		switch n := psc.Receive().(type) {
		case redis.Message:
			svc.log().Infof("%v", n)
			svc.ProcessQueue()
		case redis.PMessage:
			svc.log().Infof("%v", n)
			svc.ProcessQueue()
		case redis.Subscription:
			svc.log().Infof("Subscription: %s %s %d\n", n.Kind, n.Channel, n.Count)
			if n.Count == 0 {
				return
			}
		case error:
			svc.log().Errorf("error: %v\n", n)
			return
		}
	}

}

func (svc *service) writeResultMessage() {
	c := svc.redisPool.Get()
	if _, err := c.Do("PUBLISH", "FTPBuilderResult", time.Now().Unix()); err != nil {
		println("Error on sending result: %v\n", err)
	}
}

func (svc *service) WsNotify() {
	_, err := svc.wsClient.Publish(svc.WSConfig.Channel, []byte(`{"system": "queue_reader", "time": "`+strconv.FormatInt(time.Now().Unix(), 10)+`"}`))
	if err != nil {
		println(err.Error())
	}
}

func getVersionString(buf *bytes.Buffer) (string, error) {
	for line, err := buf.ReadString('\n'); ; line, err = buf.ReadString('\n') {
		if err != nil {
			if err == io.EOF {
				i := strings.Index(line, schemeVerString)
				if i < 0 {
					return "", errEmptyVerString
				}

				return line[0 : i+lenVerOffset], nil
			}

			return "", err
		}

		if strings.Contains(line, schemeVerString) {
			return line, nil
		}

		if line == "" {
			return "", errEmptyVerString
		}
	}
}
