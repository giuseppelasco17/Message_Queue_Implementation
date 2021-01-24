package Broker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type QueueConf struct {
	Semantic        string  `json:"semantic"`
	Timeout 		int 	`json:"timeout"`
	BrokerPort      string  `json:"broker_port"`
}

func getConfig(t int) QueueConf {
	//it unmarshal the json configuration file in a structure
	queueConf := make([]QueueConf, 1)
	var raw []byte
	var e error
	if t == 0 {
		raw, e = ioutil.ReadFile("../config.JSON")
	}
	if t == 1{
		raw, e = ioutil.ReadFile("../configtest1.json")
	}
	if e != nil {
		fmt.Println(e.Error())
		os.Exit(1)
	}
	err := json.Unmarshal(raw, &queueConf)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	return queueConf[0]
}

func messageInit(prodMessage ProdMessage) Message {
	mess := Message{
		IDProducer: prodMessage.IDProducer,
		Time:       prodMessage.Time,
		Seq:        prodMessage.Seq,
		Visibility: 1,
		Error:      0,
		Text:       prodMessage.Payload,
	}
	return mess
}