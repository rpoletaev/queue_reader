package queue_reader

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"bytes"
	"net/url"
	"strconv"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	// "strings"
	"fmt"
)

func (svc *service) GetErrors(w http.ResponseWriter, r *http.Request) {
	errors := make([]ProcessError, 10)
	var criteria bson.M
	url := r.URL.Query()
	et, err := strconv.ParseUint(url.Get("error_type"), 10, 32)
	if err == nil {
		criteria = bson.M{"error_type": et}
	}

	err = svc.mongoExec(svc.ErrorCollection, func(c *mgo.Collection) error {
		return c.Find(criteria).Sort("$natural: -1").Limit(10).All(&errors)
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

//ProccessErrorResponse Ответ для запроса ошибок обработки
type ProccessErrorResponse struct {
	ProcessError
	FileContent string
}

func (svc *service) GetErrDoc(w http.ResponseWriter, r *http.Request) {
	errDoc := ProcessError{}
	pars := r.URL.Query()
	id := bson.ObjectIdHex(pars.Get("id"))

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
	resp := ProccessErrorResponse{
		ProcessError: errDoc,
		FileContent:  string(bts),
	}

	js, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func (svc *service) ProcessErrorsHandler(w http.ResponseWriter, r *http.Request) {
	searchCriteria := getErrSerchCriteriaFromURL(r.URL.Query())
	errorsGetter := svc.getErrorsGetterFunc(searchCriteria)
	go svc.run(errorsGetter)

	w.WriteHeader(http.StatusOK)
}

func (svc *service) getErrorsGetterFunc(searchCriteria bson.M) func() (string, error) {
	return func() (string, error) {
		res := make([]ProcessError, 50)
		err := svc.mongoExec("queue_reader_process_errors", func(c *mgo.Collection) error {
			return c.Find(searchCriteria).Limit(50).All(&res)
		})

		if err != nil {
			return "", err
		}

		err = svc.mongoExec("queue_reader_process_errors", func(c *mgo.Collection) error {
			_, err = c.RemoveAll(bson.M{
				"_id": bson.M{"$in": getIDs(res)},
			})
			return err
		})

		if err != nil {
			fmt.Println("ошибка при удалении")
			fmt.Printf("%v\n", err)
		}
		return getFilePathsFromProcessErrors(res), nil
	}
}
func getIDs(pes []ProcessError) []bson.ObjectId {
	ids := make([]bson.ObjectId, len(pes))
	for i, pe := range pes {
		ids[i] = pe.ID
	}

	return ids
}

func getFilePathsFromProcessErrors(pes []ProcessError) string {
	l := len(pes)
	if l == 0 {
		return ""
	}

	fmt.Println("len(pes) ", l)
	buf := bytes.NewBufferString(pes[0].FilePath)
	if l == 1 {
		return buf.String()
	}

	for i := 1; i < len(pes); i++ {
		buf.WriteString(",")
		buf.WriteString(pes[i].FilePath)
	}

	return buf.String()
}

func getErrSerchCriteriaFromURL(pars url.Values) bson.M {
	// fPath := pars.Get("file_name")
	// startIdx := strings.LastIndex(fPath, "/")
	// endIdx := strings.Index(fPath[startIdx:], "_")

	et, _ := strconv.ParseInt(pars.Get("error_type"), 10, 32)
	// if err != nil {
	// 	return bson.M{
	// 		"file_path": "/" + fPath[startIdx:endIdx] + "/",
	// 	}
	// }

	// return bson.M{
	// 	"error_type": et,
	// 	"file_path":  "/" + fPath[startIdx:endIdx] + "/",
	// }
	return bson.M{"error_type": et}
}
