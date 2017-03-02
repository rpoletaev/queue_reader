package main

import (
	qr "github.com/rpoletaev/queue_reader"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
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
		svc.Run()
		w.Write([]byte("Сервис запущен"))
	})

	http.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) {
		svc.Stop()
		w.Write([]byte("Сервис остановлен"))
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Ай, кто к нам пришел :)"))
	})

	http.ListenAndServe(svc.Port, nil)
}
