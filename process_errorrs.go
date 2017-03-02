package queue_reader

type ProcessError struct {
	ErrorType int    `bson:"error_type"`
	FilePath  string `bson:"file_path"`
	Error     string `bson:"error"`
	CreatedAt int64  `bson:"created_at"`
}
