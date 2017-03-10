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
	"gopkg.in/mgo.v2/bson"
)

type service struct {
	*log.Logger
	*Config
	redisPool            *redis.Pool
	mongo                *mgo.Session
	errorsProcessRunning bool
	queueProcessRunning  bool
	errorsDone           chan bool //канал для остановки процесса обработки ошибок
	queueDone            chan bool //канал для остановки процесса обработки файлов из очереди
}

// GetService Возвращает настроенный из конфига сервис
func GetService(cnf *Config) (*service, error) {
	logHook, err := mgorus.NewHooker(cnf.LogHook.DBHost, cnf.LogHook.DBName, cnf.LogHook.System)
	if err != nil {
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
	for i := 0; i < svc.RoutineCount; i++ {
		if svc.errorsProcessRunning {
			svc.errorsDone <- true
		}
		if svc.queueProcessRunning {
			svc.queueDone <- true
		}
	}

	if svc.errorsProcessRunning {
		close(svc.errorsDone)
	}
	if svc.queueProcessRunning {
		close(svc.queueDone)
	}

}

// запустить обработку файлов из очереди загрузки
func (svc *service) Run() {
	if svc.queueProcessRunning {
		return
	}

	svc.queueDone = make(chan bool, svc.RoutineCount)
	svc.queueProcessRunning = true
	defer func() {
		svc.queueProcessRunning = false
	}()

	for i := 0; i < svc.RoutineCount; i++ {
		go func(routineNum int) {
			for {
				select {
				case <-svc.queueDone:
					return
				default:
					svc.processFilesList(routineNum, svc.fileListQueue)
				}
			}
		}(i)
	}
}

// запустить обработку файлов из очереди ошибок
func (svc *service) ProcessErrors() {
	if svc.errorsProcessRunning {
		return
	}

	svc.errorsDone = make(chan bool, svc.RoutineCount)
	svc.errorsProcessRunning = true
	defer func() {
		svc.errorsProcessRunning = false
	}()

	for i := 0; i < svc.RoutineCount; i++ {
		go func(routineNum int) {
			for {
				select {
				case <-svc.errorsDone:
					return
				default:
					svc.processFilesList(routineNum, svc.fileListFromErrors)
				}
			}
		}(i)
	}
}

// Обрабатываем файл:
// Получаем из источника посредством getFileFunc(), например из очереди сервиса загрузки файлов
// или из очереди ошибок для попытки поправить и сохранить в бд
// на каждом этапе обработки при возникновении ошибки пишем ошибку в специальную очередь в MongoDB
// указывая  тип ошибки, соответствующий этапу обработки. После чего выходим из функции.
// Если ни на одном из этапов обработки ошибок не возникло, то пытаемся удалить файл с диска.
// В случае неудачи так же запишем сообщение в очередь ошибок с типом ErrorRemove
func (svc *service) processFilesList(routineNum int, getListFunc func() (string, error)) {
	queueList, err := getListFunc()
	if err != nil {
		svc.log().Warning("Нет файлов в очереди")
		time.Sleep(1 * time.Minute)
		return
	}

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
	mErr := svc.mongoExec(svc.ErrorCollection, func(col *mgo.Collection) error {
		return col.Find(nil).All(&pe)
	})

	if mErr != nil {
		return "", mErr
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

// Сохраним информацию об ошибке обработки файла в очередь для последующей обработки
func (svc *service) storeFileProcessError(erType int, path string, err error) error {
	pe := ProcessError{
		ID:        bson.NewObjectId(),
		ErrorType: erType,
		FilePath:  path,
		CreatedAt: time.Now().Unix(),
		Error:     err.Error(),
	}

	mErr := svc.mongoExec(svc.ErrorCollection, func(col *mgo.Collection) error {
		return col.Insert(pe)
	})
	if mErr != nil {
		svc.log().Errorln("Ошибка при сохранении ошибки: ", mErr)
	}
	return mErr
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
