package queue_reader

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func (svc *service) GetErrors(w http.ResponseWriter, r *http.Request) {
	errors := make([]ProcessError, 10)
	err := svc.mongoExec(svc.ErrorCollection, func(c *mgo.Collection) error {
		return c.Find(nil).Sort("$natural: -1").Limit(10).All(&errors)
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	js, err := json.Marshal(errors)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func (svc *service) GetErrDoc(w http.ResponseWriter, r *http.Request) {
	errDoc := ProcessError{}
	pars := r.URL.Query()
	id := bson.ObjectIdHex(pars.Get("id"))
	fmt.Printf("%v\n", id)
	// urlSpl := strings.Split(r.URL.RawPath, "/")
	// urlSpl[len(urlSpl)-1])
	err := svc.mongoExec(svc.ErrorCollection, func(c *mgo.Collection) error {
		return c.FindId(id).One(&errDoc)
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	bts, err := ioutil.ReadFile(errDoc.FilePath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "plain/text")
	w.Write(bts)
}
