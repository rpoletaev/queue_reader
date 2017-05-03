package queue_reader

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
	expinf "github.com/rpoletaev/exportinfo"
	"github.com/weekface/mgorus"
	mgo "gopkg.in/mgo.v2"
	"runtime/debug"
	"strings"
	//"path/filepath"
	"sync"
)

var queueMutex sync.Mutex

const MaxFileSize = 16793600

type service struct {
	*log.Logger
	*Config
	redisPool *redis.Pool
	redisConn redis.Conn
	mongo     *mgo.Session
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
}

// запустить обработку файлов из очереди загрузки
func (svc *service) run(fileGetterFunc func() (string, error)) {
	if svc.Running() {
		println("Сервис уже запущен. Выходим")
		return
	}

	svc.SetRunning(true)

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

		// pathStruct.Path = path
		// svc.mongoExec("processedFiles", func(c *mgo.Collection) error {
		// 	return c.Insert(pathStruct)
		// })
		fi, _ := os.Stat(path)
		if fi.Size() > MaxFileSize {
			svc.storeFileProcessError(ErrorBigFileSize, path, fmt.Errorf("Размер файла: %d", fi.Size()))
		}

		xmlBts, err := ioutil.ReadFile(path)
		if err != nil {
			svc.storeFileProcessError(ErrorReadFile, path, err)
			fmt.Printf("Routine %d: не удалось прочесть файл: %v\n", routineNum, err)
			continue
		}

		ei, err := expinf.GetExportInfo(string(xmlBts))
		if err != nil {
			svc.storeFileProcessError(ErrorExportInfo, path, err)
			continue
		}

		url := svc.GetServiceURL(ei.Version)
		err = cli.SendData(url, xmlBts)
		if err != nil {
			sendErr := fmt.Errorf("файл %s | %s | %s | %v\n", ei.Title, ei.Version, path, err)
			fmt.Println(sendErr)
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
	var vSuffix string
	switch version {
	case "1.0":
		vSuffix = "0.9.2"
	case "4.2":
		vSuffix = "4.4"
	case "4.3":
		vSuffix = "4.4"
	case "4.3.100":
		vSuffix = "4.4"
	default:
		vSuffix = version
	}

	url := fmt.Sprintf("http://%s%s:%s/load", svc.ServicePreffix, vSuffix, svc.ServicePort)
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
