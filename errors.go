package queue_reader

import (
	"strings"

	mgo "gopkg.in/mgo.v2"
)

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
