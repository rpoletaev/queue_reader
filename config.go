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
}

// MongoLogHookConfig описывает структуру для записи логов в MongoDB
type MongoLogHookConfig struct {
	Host       string `yaml:"log_host"`
	System     string `yaml:"log_system"`
	DBHost     string `yaml:"log_db_host"`
	DBName     string `yaml:"log_dbname"`
	Collection string `yaml:"log_collection"`
}
