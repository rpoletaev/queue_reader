package queue_reader

import (
	"fmt"
	_ "gopkg.in/yaml.v2"
	"regexp"
)

const (
	ErrorReadFile   = iota //Ошибка чтения файла из зеркала
	ErrorExportInfo        //Ошибка получения информации о версии и заголовке коллекции
	ErrorSend              //Ошибка возникшая, при отправке данных сервису сохранения
	ErrorRemove            //Ошибка при удалении обработанного файла
)

// Config описывает структуру конфигурации сервиса
type Config struct {
	LogHook         *MongoLogHookConfig `yaml:"log_hook"`
	MySQL           string              `yaml:"mysql"`
	XMLMirror       string              `yaml:"xml_mirror"`
	Port            string              `yaml:"port"`
	Mongo           string              `yaml:"mongo"`
	MongoDB         string              `yaml:"mongo_db"`
	ErrorCollection string              `yaml:"mongo_err_collection"`
	RedisAddress    string              `yaml:"redis_address"`
	RoutineCount    int                 `yaml:"routine_count"`
	ServicePreffix  string              `yaml:"service_preffix"`
	ServicePort     string              `yaml:"service_port"`
	NeedLogingURL   bool                `yaml:"loging_url"`
}

// MongoLogHookConfig описывает структуру для записи логов в MongoDB
type MongoLogHookConfig struct {
	Host       string `yaml:"log_host"`
	System     string `yaml:"log_system"`
	DBName     string `yaml:"log_dbname"`
	Collection string `yaml:"log_collection"`
}

// ExportInfo структура, которая содержит информацию
// о заголовке документа экспорта и версию схемы данных
// по заголовку документа определяется имя коллекции в которой будет сохранен документа
// так же по заголовку документа определяется какой именно документ из структуры экспорта мы должны получить,
// т.к. документ экспорта включает в себя все типы документов, но каждый xml содержит только один документ
// из всех возможных описаных в структуре экспорта
type ExportInfo struct {
	Title   string
	Version string
}

func getExportInfo(xmlContent string) (*ExportInfo, error) {
	r, err := regexp.Compile("<(.+:)*([a-zA-Z]+) schemeVersion=\"(.+?)\">")
	if err != nil {
		return nil, err
	}

	results := r.FindStringSubmatch(xmlContent)
	if len(results) < 4 {
		return nil, fmt.Errorf("Wrong format export file, could not determine version and title")
	}

	return &ExportInfo{Title: results[2], Version: results[3]}, nil
}
