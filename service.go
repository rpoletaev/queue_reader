package queue_reader

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
	expinf "github.com/rpoletaev/exportinfo"
	"github.com/weekface/mgorus"
	mgo "gopkg.in/mgo.v2"
)

type service struct {
	*log.Logger
	*Config
	redisPool *redis.Pool
	mongo     *mgo.Session
	running   bool
	done      chan bool
	flist     chan string
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

	svc.redisPool = newRedisPool(cnf.RedisAddress)
	svc.setupMongo()
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
	svc.done <- true
	close(svc.done)
}

// запустить обработку файлов из очереди загрузки
func (svc *service) run(fileGetterFunc func() (string, error)) {
	if svc.running {
		return
	}

	svc.done = make(chan bool, 1)
	svc.flist = make(chan string, svc.RoutineCount)

	svc.running = true
	defer func() {
		svc.running = false
	}()

	go func() {
		defer func() {
			close(svc.flist)
			svc.log().Infoln("Exit from processing queue...")
		}()

		for {
			select {
			case <-svc.done:
				return
			default:
				queueList, err := fileGetterFunc()
				if err != nil && err == redis.ErrNil {
					svc.log().Warning("Нет файлов в очереди")
					svc.Stop()
				}

				svc.flist <- queueList
			}
		}
	}()

	for i := 0; i < svc.RoutineCount; i++ {
		go func(routineNum int) {
			for {
				select {
				case list := <-svc.flist:
					svc.processFilesList(routineNum, list)
				}
			}
		}(i)
	}
}

func (svc *service) ProcessQueue() {
	svc.run(svc.fileListQueue)
}

func (svc *service) ProcessErrors() {
	svc.run(svc.fileListFromErrors)
}

// Обрабатываем файл:
// Получаем из источника посредством getFileFunc(), например из очереди сервиса загрузки файлов
// или из очереди ошибок для попытки поправить и сохранить в бд
// на каждом этапе обработки при возникновении ошибки пишем ошибку в специальную очередь в MongoDB
// указывая  тип ошибки, соответствующий этапу обработки. После чего выходим из функции.
// Если ни на одном из этапов обработки ошибок не возникло, то пытаемся удалить файл с диска.
// В случае неудачи так же запишем сообщение в очередь ошибок с типом ErrorRemove
func (svc *service) processFilesList(routineNum int, queueList string) {

	list := strings.Split(queueList, ",")
	for _, str := range list {
		if str == "" {
			continue
		}
		//svc.log().Infof("Routine %d: Файл из очереди: %s\n", routineNum, str)
		xmlBts, err := ioutil.ReadFile(str)
		if err != nil {
			svc.storeFileProcessError(ErrorReadFile, str, err)
			svc.Errorf("Routine %d: не удалось прочесть файл: %v\n", routineNum, err)
			return
		}

		ei, err := expinf.GetExportInfo(string(xmlBts))
		if err != nil {
			svc.storeFileProcessError(ErrorExportInfo, str, err)
			svc.log().Errorf("Routine %d: Не удалось прочесть версию и коллекцию из файла %s: %v\n", routineNum, str, err)
			return
		}

		cli := GetClient(time.Duration(svc.ClientTimeOut) * time.Second)
		url := svc.GetServiceURL(ei.Version)
		err = cli.SendData(url, xmlBts)
		if err != nil {
			svc.storeFileProcessError(ErrorSend, str, err)
			svc.log().Errorf("Routine %d: Ошибка при отправке данных на сервис %s: %v\n", routineNum, url, err)
			return
		}

		//svc.log().Info("Пытаемся удалить обработанный и сохраненный файл: ", str)
		if err := os.Remove(str); err != nil {
			svc.storeFileProcessError(ErrorRemove, str, err)
			svc.log().Errorf("Routine %d: Не удалось удалить файл: %v", routineNum, err)
		} //else
		// {
		// 	svc.log().Infoln("Файл удалён")
		// }
	}
}

// Получаем путь к файлу из очереди сервиса загрузки
func (svc *service) fileListQueue() (string, error) {
	conn := svc.redisPool.Get()
	defer conn.Close()
	return redis.String(conn.Do("RPOP", "FileQueue"))
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
	vSuffix := version
	if version == "1.0" {
		vSuffix = "0.9.2"
	}

	url := fmt.Sprintf("http://%s%s:%s/load", svc.ServicePreffix, vSuffix, svc.ServicePort)
	if svc.NeedLogingURL {
		svc.log().Info(url)
	}
	return url
}

// func newRedisPool(addr string) *redis.Pool {
// 	return &redis.Pool{
// 		MaxIdle:     3,
// 		IdleTimeout: 240 * time.Second,
// 		Dial: func() (redis.Conn, error) {
// 			return redis.Dial("tcp", addr, redis.DialDatabase(0))
// 		},
// 	}
// }

// func (svc *service) setupMongo() {
// 	mongo, err := mgo.Dial(svc.Mongo)
// 	if err != nil {
// 		panic(err)
// 	}

// 	svc.mongo = mongo
// 	svc.mongo.SetMode(mgo.Monotonic, true)
// }

// func (svc *service) mongoExec(colectionName string, execFunc func(*mgo.Collection) error) error {
// 	session := svc.mongo.Clone()
// 	defer session.Close()

// 	db := session.DB(svc.MongoDB)
// 	collection := db.C(colectionName)
// 	return execFunc(collection)
// }

// func (svc *service) ClearQueue() error {
// 	conn := svc.redisPool.Get()
// 	defer conn.Close()
// 	_, err := conn.Do("DEL", "FileQueue")
// 	return err
// }

// func (svc *service) ClearErrorQueue() (removed int, err error) {
// 	svc.mongoExec(svc.ErrorCollection, func(c *mgo.Collection) error {
// 		i, err := c.RemoveAll(nil)
// 		if err != nil {
// 			return err
// 		}

// 		removed = i.Removed
// 		return nil
// 	})
// 	return removed, err
// }
