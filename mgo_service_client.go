package queue_reader

import (
	"bytes"
	"net/http"
	"time"
)

type client struct {
	client *http.Client
}

// GetClient возвращает http.Client с установленным таймаутом в 10 секунд
func GetClient() *client {
	return &client{
		client: &http.Client{Timeout: time.Duration(10 * time.Second)},
	}
}

// SendData отправляет массив байтов на сервис
func (cli *client) SendData(url string, data []byte) error {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
	defer req.Body.Close()
	req.Header.Set("Content-Type", "application/xml")

	_, err = cli.client.Do(req)
	return err
	// io.Copy(os.Stdout, resp.Body)
	// defer resp.Body.Close()
}
