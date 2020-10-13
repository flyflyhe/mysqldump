package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
)

type Msg struct {
	Code int `json:"code"`
	Msg string `json:"msg"`
}

func (msg Msg) Error() string  {
	return msg.Msg
}

func Notify(path string, file string, isZip int, md5 string) error {
	resp, err := http.PostForm(path, url.Values{"file": {file}, "isZip":{string(isZip)}, "md5": {md5}})
	if err != nil{
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return Msg{Msg:"not 200"}
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	log.Println(string(body))
	var msg Msg

	err = json.Unmarshal(body, &msg)
	if err != nil {
		return err
	}

	if msg.Code == 1 {
		return msg
	}

	return nil
}
