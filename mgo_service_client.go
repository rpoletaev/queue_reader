package queue_reader

import (
	"bytes"
	"errors"
	"net/http"
	"time"
)

type client struct {
	client *http.Client
}

// GetClient возвращает http.Client с установленным таймаутом в 10 секунд
func GetClient(timeOut time.Duration) *client {
	return &client{
		client: &http.Client{Timeout: timeOut},
	}
}

// SendData отправляет массив байтов на сервис
func (cli *client) SendData(url string, data []byte) error {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer req.Body.Close()

	// req.Header.Set("Content-Type", "application/xml")
	resp, err := cli.client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		respBuf := bytes.NewBuffer(make([]byte, 0, resp.ContentLength))
		respBuf.ReadFrom(resp.Body)
		resp.Body.Close()
		return errors.New(respBuf.String())
	}
	return nil
}
