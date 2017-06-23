package main

import (
	"io/ioutil"
	"log"
	"net/http"

	qr "github.com/rpoletaev/queue_reader"
	"gopkg.in/yaml.v2"
)

func main() {
	cnfBts, err := ioutil.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("Ошбка при чтении конфига: %v", err)
	}

	cfg := &qr.Config{}
	err = yaml.Unmarshal(cnfBts, cfg)
	if err != nil {
		log.Fatalf("Ошбка конфигурации %v", err)
	}

	svc, err := qr.GetService(cfg)
	if err != nil {
		log.Fatalf("Ошбка инициализации сервиса %v", err)
	}

	http.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		if svc == nil {
			w.Write([]byte("Сервис не инициализирован"))
		}
		go svc.ProcessQueue()
		w.Write([]byte("Сервис запущен"))
	})

	http.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) {
		svc.Stop()
		w.Write([]byte("Сервис остановлен"))
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Ай, кто к нам пришел :)"))
	})

	// http.HandleFunc("/clear_queue", func(w http.ResponseWriter, r *http.Request) {
	// 	if err := svc.ClearQueue(); err != nil {
	// 		w.Write([]byte(err.Error()))
	// 		return
	// 	}
	// 	w.Write([]byte("Очередь очищена"))
	// })

	http.HandleFunc("/get_errors", svc.GetErrors)
	http.HandleFunc("/showDocument", svc.GetErrDoc)
	http.HandleFunc("/process_document", svc.ProcessUserDocument)
	http.HandleFunc("/process_errors", svc.ProcessErrorsHandler)

	// http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
	// 	w.Write([]byte("test"))
	// 	svc.TestCall()
	// })
	http.ListenAndServe(svc.Port, nil)
}
