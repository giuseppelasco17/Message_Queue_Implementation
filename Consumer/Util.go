package Consumer

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

type Payload struct {
	Payload        string  `json:"payload"`
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

func marshalMessage(arg Message){
	//in test case save the message into a json file
	dat := Payload{
		Payload: arg.Text,
	}
	data := make([]Payload, 1)
	data[0] = dat
	file, _ := json.MarshalIndent(data, "", " ")

	_ = ioutil.WriteFile("../resultTest.json", file, 0644)
}