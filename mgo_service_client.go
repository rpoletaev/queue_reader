package queue_reader

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
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
func (cli *client) SendData(url string, content XmlContent) error {
	js, err := json.Marshal(content)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(js))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := cli.client.Do(req)
	req.Body.Close()
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		respBuf := bytes.NewBuffer(make([]byte, 0, resp.ContentLength))
		respBuf.ReadFrom(resp.Body)
		defer func() {
			resp.Body.Close()
			respBuf.Truncate(0)
		}()

		return errors.New(respBuf.String())
	}
	return nil
}

// XmlToJSON send xmlContent to service and return JSON
func (cli *client) XmlToJSON(url string, content XmlContent) ([]byte, error) {
	js, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(js))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := cli.client.Do(req)
	defer req.Body.Close()
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		respBuf := bytes.NewBuffer(make([]byte, 0, resp.ContentLength))
		respBuf.ReadFrom(resp.Body)
		defer func() {
			resp.Body.Close()
			respBuf.Truncate(0)
		}()

		return nil, errors.New(respBuf.String())
	}

	return ioutil.ReadAll(resp.Body)
}
