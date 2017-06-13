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
	//"io"
	"sync"
)

type XmlContent struct {
	DocType string `json:"docType"`
	Content []byte `json:"content"`
}

var wg sync.WaitGroup
var serviceURLMu sync.Mutex
var verMap map[string]string
var errEmptyVerString = errors.New("Не удалось найти строку с версией")

const linkTemplate = "http://%s%s:%s/load"
const errDataSendTemplate = "файл %s | %s | %s | %v"
const schemeVerString = "schemeVersion"
const lenVerOffset = 200

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
	// for i := 0; i < svc.RoutineCount; i++ {
	// 	svc.done <- true
	// }
	// close(svc.done)
	svc.SetRunning(false)
}

// запустить обработку файлов из очереди загрузки
func (svc *service) run(fileGetterFunc func() (string, error)) {
	if svc.Running() {
		println("Сервис уже запущен. Выходим")
		return
	}

	svc.SetRunning(true)
	svc.WsNotify("endProcess", strconv.FormatInt(time.Now().Unix(), 10))

	wg.Add(svc.RoutineCount)

	svc.done = make(chan bool, svc.RoutineCount)
	svc.flist = make(chan string, svc.RoutineCount)
	rc, err := redis.Dial("tcp", svc.RedisAddress, redis.DialDatabase(0), redis.DialPassword(svc.RedisPassword))
	if err != nil {
		panic(err)
	}

	svc.redisConn = rc

	go func() {
		defer close(svc.flist)
		for {
			queueList, err := fileGetterFunc()
			if err != nil {
				// svc.Stop()
				return
			}

			for _, p := range strings.Split(queueList, ",") {
				svc.flist <- p
			}
		}
	}()

	go func() {
		for i := 0; i < svc.RoutineCount; i++ {
			go func(routineNum int) {
				svc.runProcessWorker(svc.flist)
				println("Выходим из процесса #", routineNum)
			}(i)
		}
	}()

	wg.Wait()
	debug.FreeOSMemory()
	svc.writeResultMessage()
	svc.redisConn.Close()
	svc.SetRunning(false)
}

func (svc *service) runProcessWorker(paths <-chan string) {
	defer wg.Done()
	cli := GetClient(time.Duration(svc.ClientTimeOut) * time.Second)
	for path := range paths {
		svc.ProcessFile(path, cli)
	}
}

func (svc *service) ProcessQueue() {
	svc.run(svc.fileListQueue)
}

// Обрабатываем файл:
// Получаем из источника посредством getFileFunc(), например из очереди сервиса загрузки файлов
// или из очереди ошибок для попытки поправить и сохранить в бд
// на каждом этапе обработки при возникновении ошибки пишем ошибку в специальную очередь в MongoDB
// указывая  тип ошибки, соответствующий этапу обработки. После чего выходим из функции.
// Если ни на одном из этапов обработки ошибок не возникло, то пытаемся удалить файл с диска.
// В случае неудачи так же запишем сообщение в очередь ошибок с типом ErrorRemove
func (svc *service) ProcessFile(path string, c *client) {
	if path == "" {
		return
	}

	xmlBts, err := ioutil.ReadFile(path)
	if len(xmlBts) > MaxFileSize {
		svc.storeFileProcessError(ErrorBigFileSize, path, fmt.Errorf("Размер файла: %d", len(xmlBts)))
	}

	if err != nil {
		svc.storeFileProcessError(ErrorReadFile, path, err)
		fmt.Printf("Не удалось прочесть файл: %v\n", err)
		return
	}

	buf := bytes.NewBuffer(xmlBts)
	verStr, err := GetVersionString(buf, DocTypeFromPath(path))
	if err != nil {
		fmt.Println("Error version string ", err.Error(), " ", path)
		svc.storeFileProcessError(ErrorExportInfo, path, err)
		return
	}

	ei, err := expinf.GetExportInfoByTag(verStr) //string(xmlBts[0 : len(xmlBts)/3])
	if err != nil {
		svc.storeFileProcessError(ErrorExportInfo, path, err)
		return
	}

	content := XmlContent{
		DocType: ei.Title,
		Content: xmlBts,
	}

	url := svc.GetServiceURL(ei.Version)
	err = c.SendData(url, content)
	xmlBts = nil

	if err != nil {
		sendErr := fmt.Errorf(errDataSendTemplate, ei.Title, ei.Version, path, err)
		svc.storeFileProcessError(ErrorSend, path, sendErr)
		return
	}

	if err := os.Remove(path); err != nil {
		svc.storeFileProcessError(ErrorRemove, path, err)
		return
	}
}

// Получаем путь к файлу из очереди сервиса загрузки
func (svc *service) fileListQueue() (string, error) {
	return redis.String(svc.redisConn.Do("SPOP", "FileQueue"))
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
			// svc.log().Infof("Subscription: %s %s %d\n", n.Kind, n.Channel, n.Count)
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
		svc.log().Errorf("Error on sending result: %v\n", err)
	}
}

func (svc *service) WsNotify(msgType string, message string) {
	_, err := svc.wsClient.Publish(svc.WSConfig.Channel, []byte(`{"system": "queue_reader", "mt": "`+msgType+`", "message": "`+message+`"}`))
	if err != nil {
		println(err.Error())
	}
}

//ищет строку содержащую schemeVerString
func GetVersionString(buf *bytes.Buffer, docType string) (string, error) {
	docTypeIndex := -1

	for line, err := buf.ReadString('>'); ; line, err = buf.ReadString('>') {
		if strings.Contains(line, schemeVerString) {
			return strings.TrimSpace(line), nil
		}

		docTypeIndex = strings.Index(line, docType)
		if docTypeIndex > 0 {
			schemeVer := " " + schemeVerString + `="7.1"`
			res := make([]byte, len(line)+len(schemeVer))
			docTypeLastIndex := docTypeIndex + len(docType)

			copy(res[:docTypeLastIndex], line[:docTypeLastIndex])
			res = append(res, []byte(schemeVer)...)
			res = append(res, line[docTypeLastIndex:]...)
			return strings.TrimSpace(string(res)), nil
		}

		if err != nil {
			return "", err
		}
	}
}

func DocTypeFromPath(path string) string {
	startIndex := strings.LastIndex(path, "/") + 1
	lastIndex := strings.Index(path[startIndex:], "_")
	return path[startIndex : startIndex+lastIndex]
}
