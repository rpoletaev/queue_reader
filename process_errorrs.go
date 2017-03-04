package queue_reader

import (
	"gopkg.in/mgo.v2/bson"
)

type ProcessError struct {
	ID        bson.ObjectId `bson:"_id"`
	ErrorType int           `bson:"error_type"`
	FilePath  string        `bson:"file_path"`
	Error     string        `bson:"error"`
	CreatedAt int64         `bson:"created_at"`
}
