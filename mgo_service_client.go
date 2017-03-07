package queue_reader

import (
	"bytes"
	"errors"
	"fmt"
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
	if err != nil {
		return err
	}
	defer req.Body.Close()

	// req.Header.Set("Content-Type", "application/xml")
	fmt.Printf("REQUEST_URL: %+v", url)
	resp, err := cli.client.Do(req)
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		respBuf := bytes.NewBuffer(make([]byte, 0, resp.ContentLength))
		respBuf.ReadFrom(resp.Body)
		return errors.New(respBuf.String())
	}
	return nil
}
