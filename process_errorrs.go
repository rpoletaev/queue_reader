package queue_reader

import (
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// ProcessError структура описывает ошибку обработки файла:
// тип ошибки, путь к файлу, текст ошибки, дату и время возникновения
type ProcessError struct {
	ID        bson.ObjectId `bson:"_id"`
	ErrorType int           `bson:"error_type"`
	FilePath  string        `bson:"file_path"`
	Error     string        `bson:"error"`
	CreatedAt int64         `bson:"created_at"`
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
		svc.log().Errorln("Не удалось сохранить информацию об ошибке обработки файла: ", mErr)
	}
	return mErr
}
