package queue_reader

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"bytes"
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/centrifugal/gocent"
	"github.com/garyburd/redigo/redis"
	"github.com/jinzhu/gorm"
	expinf "github.com/rpoletaev/exportinfo"
	"github.com/weekface/mgorus"
	"gitlab.com/garanteka/goszakupki/ftpworker"
	mgo "gopkg.in/mgo.v2"
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
	mysql     *gorm.DB
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

	svc.setupMongo()
	svc.setupMysql()
	svc.setupRedis()
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
func (svc *service) run(fileGetterFunc func() ([]ftpworker.ArchFile, error)) { //, endCallBack func()
	if svc.Running() {
		println("Сервис уже запущен. Выходим")
		return
	}
	find := make(map[string]int)
	svc.SetRunning(true)

	wg.Add(svc.RoutineCount)

	//svc.flist = make(chan string, svc.RoutineCount)
	// svc.redisConn = svc.redisPool.Get()
	// defer svc.redisConn.Close()
	files, err := fileGetterFunc()

	if err != nil {
		svc.log().Error(err)
		return
	}

	flist := make(chan ftpworker.ArchFile, len(files))
	//читаем из очереди списки путей, разделенных запятыми, разбиваем на отдельные пути и шлем в канал обработки файлов,
	//при выходе закрываем канал обработки
	go func() {
		defer func() {
			close(svc.flist)
			for k, v := range find {
				fmt.Printf("%s %d\n", k, v)
			}
		}()

		for _, file := range files {
			// if err != nil {
			// 	println(err.Error())
			// 	return
			// }
			find[file.File]++
			// paths := strings.Split(queueList, ",")
			// for _, p := range paths {
			flist <- file
			// }
		}
		close(flist)
	}()

	batchChan := make(chan ftpworker.ArchFile, svc.RoutineCount)
	go batch(svc.mysql, batchChan)

	//Запустим routineNum процессов читающих из svc.flist и обрабатывающих прочитанные файлы
	for i := 0; i < svc.RoutineCount; i++ {
		go func(routineNum int) {
			defer func() {
				println("Выходим из процесса #", routineNum)
				wg.Done()
			}()

			cli := GetClient(time.Duration(svc.ClientTimeOut) * time.Second)
			for f := range flist {
				svc.ProcessFile(f, cli, batchChan)
			}
		}(i)
	}

	// ждем окончания обработки всех переданных файлов
	wg.Wait()
	close(batchChan)
	debug.FreeOSMemory()

	// отправим сообщение об окончании на веб-морду
	svc.WsNotify("endProcess", strconv.FormatInt(time.Now().Unix(), 10))
	// if endCallBack != nil {
	// 	endCallBack()
	// }
	svc.writeResultMessage()
	svc.SetRunning(false)
}

func (svc *service) ProcessQueue() {
	svc.run(svc.mysqlListQueue)
	// , func() {
	// 	//отправим сообщение об окончании в очередь загрузки

	// 	svc.writeResultMessage()
	// })
}

// Обрабатываем файл:
// Получаем из источника посредством getFileFunc(), например из очереди сервиса загрузки файлов
// или из очереди ошибок для попытки поправить и сохранить в бд
// на каждом этапе обработки при возникновении ошибки пишем ошибку в специальную очередь в MongoDB
// указывая  тип ошибки, соответствующий этапу обработки. После чего выходим из функции.
// Если ни на одном из этапов обработки ошибок не возникло, то пытаемся удалить файл с диска.
// В случае неудачи так же запишем сообщение в очередь ошибок с типом ErrorRemove
func (svc *service) ProcessFile(file ftpworker.ArchFile, c *client, batchChannel chan ftpworker.ArchFile) {

	if file.File == "" {
		return
	}

	xmlBts, err := ioutil.ReadFile(file.File)
	if len(xmlBts) > MaxFileSize {
		svc.storeFileProcessError(ErrorBigFileSize, file.File, fmt.Errorf("Размер файла: %d", len(xmlBts)))
	}

	if err != nil {
		svc.storeFileProcessError(ErrorReadFile, file.File, err)
		fmt.Printf("Не удалось прочесть файл: %v\n", err)
		return
	}

	buf := bytes.NewBuffer(xmlBts)
	verStr, err := GetVersionString(buf, DocTypeFromPath(file.File))
	if err != nil {
		fmt.Println("Error version string ", err.Error(), " ", file.File)
		svc.storeFileProcessError(ErrorExportInfo, file.File, err)
		return
	}

	ei, err := expinf.GetExportInfoByTag(verStr) //string(xmlBts[0 : len(xmlBts)/3])
	if err != nil {
		svc.storeFileProcessError(ErrorExportInfo, file.File, err)
		return
	}

	content := XmlContent{
		DocType: ei.Title,
		Content: xmlBts,
	}

	url := svc.GetServiceURL(linkTemplate, ei.Version)
	err = c.SendData(url, content)
	xmlBts = nil

	if err != nil {
		sendErr := fmt.Errorf(errDataSendTemplate, ei.Title, ei.Version, file.File, err)
		svc.storeFileProcessError(ErrorSend, file.File, sendErr)
		return
	}

	if err := os.Remove(file.File); err != nil {
		svc.storeFileProcessError(ErrorRemove, file.File, err)
		return
	}

	batchChannel <- file
}

var rMu sync.Mutex

// Получаем путь к файлу из очереди сервиса загрузки в redis
func (svc *service) redisListQueue() (string, error) {
	rMu.Lock()
	defer rMu.Unlock()
	return redis.String(svc.redisConn.Do("SPOP", "FileQueue"))
}

// Получаем путь к файлу из очереди сервиса загрузки в mysql
func (svc *service) mysqlListQueue() ([]ftpworker.ArchFile, error) {
	var archFiles []ftpworker.ArchFile
	err := svc.mysql.Raw("SELECT * FROM arch_files WHERE downloaded=0").Scan(&archFiles).Error
	return archFiles, err
}

// GetServiceURL принимает версию данных и формирует url для сервиса
func (svc service) GetServiceURL(template, version string) string {
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

	url := fmt.Sprintf(template, svc.ServicePreffix, verMap[version], svc.ServicePort)
	if svc.NeedLogingURL {
		svc.log().Info(url)
	}
	return url
}

func (svc *service) runRedisSubscribe() {
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

// GetVersionString ищет строку содержащую schemeVerString
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

// DocTypeFromPath получает тип документа из пути к файлу
func DocTypeFromPath(path string) string {
	startIndex := strings.LastIndex(path, "/") + 1
	lastIndex := strings.Index(path[startIndex:], "_")
	nameFromPath := path[startIndex : startIndex+lastIndex]
	if nameFromPath == "fcs" {
		prevLastIndex := startIndex + lastIndex
		lastIndex = strings.Index(path[prevLastIndex+1:], "_")
		nameFromPath = path[startIndex : prevLastIndex+lastIndex+1]
	}

	switch nameFromPath {
	case "fcsRegulationRulesInvalid":
		return "fcsRegulationRules"
	default:
		return nameFromPath
	}
}

func (svc *service) setupMysql() {
	b, err := gorm.Open("mysql", svc.MysqlConString)
	if err != nil {
		panic(err)
	}

	svc.mysql = b
}

func (svc *service) setupRedis() {
	svc.redisPool = newRedisPool(svc.Config.RedisAddress, svc.Config.RedisPassword)
	go svc.runRedisSubscribe()
}

func batch(db *gorm.DB, bc <-chan ftpworker.ArchFile) {
	buf := bytes.NewBufferString("UPDATE arch_files SET downloaded=1 WHERE id in ( ")
	var ids []string
	for f := range bc {
		ids = append(ids, strconv.Itoa(int(f.ID)))
	}

	buf.WriteString(strings.Join(ids, ","))
	buf.WriteString(")")

	q := buf.String()
	err := db.Exec(q).Error
	if err != nil {
		panic(err)
	}

}
