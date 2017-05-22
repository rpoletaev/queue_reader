package queue_reader

import (
	mgo "gopkg.in/mgo.v2"
	// "gopkg.in/mgo.v2/bson"
	"encoding/json"
	"net/http"
)

func (svc *service) GetErrors(w http.ResponseWriter, r *http.Request) {
	errors := make([]string, 10)
	svc.mongoExec(svc.ErrorCollection, func(c *mgo.Collection) error {
		return c.Find(nil).Sort("$natural: -1").Limit(10).All(&errors)
	})

	js, err := json.Marshal(errors)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}
