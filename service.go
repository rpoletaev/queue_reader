package queue_reader

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
	"github.com/weekface/mgorus"
	mgo "gopkg.in/mgo.v2"
	"io/ioutil"
	"os"
	"time"
)

type service struct {
	*log.Logger
	*Config
	redisPool *redis.Pool
	mongo     *mgo.Session
	done      chan bool
}

func GetService(cnf *Config) (*service, error) {
	logHook, err := mgorus.NewHooker(cnf.LogHook.Host, cnf.LogHook.DBName, cnf.LogHook.System)
	if err != nil {
		return nil, err
	}

	svc := &service{}
	svc.Config = cnf

	svc.Logger = log.New()
	svc.Hooks.Add(logHook)

	svc.redisPool = newRedisPool(cnf.RedisAddress)
	svc.setupMongo()
	svc.done = make(chan bool, svc.RoutineCount)
	return svc, nil
}

func (svc *service) log() *log.Entry {
	return svc.Logger.WithFields(log.Fields{
		"system": svc.LogHook.System,
		"host":   svc.LogHook.Host,
	})
}

func (svc *service) Run() {
	for i := 0; i < svc.RoutineCount; i++ {
		go func(routineNum int) {
			for {
				select {
				case <-svc.done:
					return
				default:
					str, err := svc.fileFromQueue()
					if err != nil {
						svc.log().Warning("Нет файлов в очереди")
						svc.log().Errorf("Routine %d: %v\n", routineNum, err)
						time.Sleep(1 * time.Minute)
						continue
					}

					svc.log().Infof("Routine %d: Файл из очереди: %s\n", routineNum, str)
					xmlBts, err := ioutil.ReadFile(str)
					if err != nil {
						svc.storeFileProcessError(ErrorReadFile, str, err)
						svc.Errorf("Routine %d: не удалось прочесть файл: %v\n", routineNum, err)
						continue
					}

					ei, err := getExportInfo(string(xmlBts))
					if err != nil {
						svc.storeFileProcessError(ErrorExportInfo, str, err)
						svc.log().Errorf("Routine %d: Не удалось прочесть версию и коллекцию из файла %s: %v\n", routineNum, str, err)
						continue
					}

					cli := GetClient()
					url := svc.GetServiceURL(ei.Version)
					err = cli.SendData(url, xmlBts)
					if err != nil {
						svc.storeFileProcessError(ErrorSend, str, err)
						svc.log().Errorf("Routine %d: Ошибка при отправке данных на сервис %s: %v", routineNum, url, err)
					}

					svc.log().Info("Пытаемся удалить обработанный и сохраненный файл: ", str)
					if err := os.Remove(str); err != nil {
						svc.storeFileProcessError(ErrorRemove, str, err)
						svc.log().Errorf("Routine %d: Не удалось удалить файл: %v", routineNum, err)
					} else {
						svc.log().Infoln("Файл удалён")
					}
				}
			}
		}(i)
	}
}

func (svc *service) storeFileProcessError(erType int, path string, err error) error {
	pe := ProcessError{
		ErrorType: erType,
		FilePath:  path,
		CreatedAt: time.Now().Unix(),
		Error:     err.Error(),
	}

	mErr := svc.mongoExec(svc.ErrorCollection, func(col *mgo.Collection) error {
		return col.Insert(pe)
	})
	return mErr
}

// GetServiceURL принимает версию данных и формирует url для сервиса
func (svc service) GetServiceURL(version string) string {
	vSuffix := version
	if version == "1.0" {
		vSuffix = "0.9.2"
	}

	return fmt.Sprintf("http://%s%s:%s/load/", svc.ServicePreffix, vSuffix, svc.ServicePort)
}

func (svc *service) Stop() {
	for i := 0; i < svc.RoutineCount; i++ {
		svc.done <- true
	}
	close(svc.done)

	time.Sleep(5 * time.Second)
	svc.redisPool.Close()
	svc.mongo.Close()
}

func newRedisPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr, redis.DialDatabase(0), redis.DialPassword("nigersex"))
		},
	}
}

func (svc *service) fileFromQueue() (string, error) {
	conn := svc.redisPool.Get()
	defer conn.Close()
	return redis.String(conn.Do("RPOP", "FileQueue"))
}

func (svc *service) setupMongo() {
	mongo, err := mgo.Dial(svc.Mongo)
	if err != nil {
		panic(err)
	}

	svc.mongo = mongo
	svc.mongo.SetMode(mgo.Monotonic, true)
}

func (svc *service) mongoExec(colectionName string, execFunc func(*mgo.Collection) error) error {
	session := svc.mongo.Clone()
	defer session.Close()

	db := session.DB(svc.MongoDB)
	collection := db.C(colectionName)
	return execFunc(collection)
}

func (svc *service) ClearQueue() error {
	conn := svc.redisPool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", "FileQueue")
	return err
}
