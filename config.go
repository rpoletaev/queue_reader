package queue_reader

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
	RedisPassword   string              `yaml:"redis_password"`
	RoutineCount    int                 `yaml:"routine_count"`
	ServicePreffix  string              `yaml:"service_preffix"`
	ServicePort     string              `yaml:"service_port"`
	NeedLogingURL   bool                `yaml:"loging_url"`
	ClientTimeOut   int                 `yaml:"client_timeout"`
	WSConfig        *CentrifugoConfig   `yaml:"ws_config"`
}

// MongoLogHookConfig описывает структуру для записи логов в MongoDB
type MongoLogHookConfig struct {
	Host       string `yaml:"log_host"`
	System     string `yaml:"log_system"`
	DBHost     string `yaml:"log_db_host"`
	DBName     string `yaml:"log_dbname"`
	Collection string `yaml:"log_collection"`
}

//CentrifugoConfig описывает структуру для настройки работы с centrifugo
type CentrifugoConfig struct {
	Secret  string `yaml:"ws_secret"`
	Host    string `yaml:"ws_host"`
	Port    string `yaml:"ws_port"`
	Channel string `yaml:"ws_channel"`
}
