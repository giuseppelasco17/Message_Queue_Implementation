package Producer

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"time"
)

type QueueConf struct {
	Semantic        string  `json:"semantic"`
	Timeout 		int 	`json:"timeout"`
	BrokerPort      string  `json:"broker_port"`
	ProducerPort    string  `json:"producer_port"`
}

type Payload struct {
	Payload        string  `json:"payload"`
}

type ProdMessage struct {//the messages sent to the broker
	IDProducer int
	Time string
	Seq int
	Payload string
}


func createID() {
	//the producer ID is the time hash simply
	t := time.Now()
	stringTime := t.String()
	h := fnv.New32a()
	_, err := h.Write([]byte(stringTime))
	if err != nil {
		log.Fatal("error hash", err)
	}
	IDProd = int(h.Sum32())
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
	if t == 2{
		raw, e = ioutil.ReadFile("../configtest2.json")
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

func getMessages(IDProd int, t int) []ProdMessage {
	//it reads the json file and builds the messages
	var payloads []Payload
	var raw []byte
	var er error
	if t == 0{
		raw, er = ioutil.ReadFile("../Producer/messages.JSON")
	}
	if t == 1{
		raw, er = ioutil.ReadFile("../Producer/test.JSON")
	}
	if er != nil {
		fmt.Println(er.Error())
		os.Exit(1)
	}
	er = json.Unmarshal(raw, &payloads)
	if er != nil {
		fmt.Println(er.Error())
		os.Exit(1)
	}
	var messages []ProdMessage
	for i := 0; i < len(payloads); i++{
		mess := createMessage(IDProd, i, payloads[i].Payload)
		messages = append(messages, mess)
	}
	return messages
}

func createMessage(id int, seq int, payload string) ProdMessage {
	t := time.Now()
	stringTime := t.String()
	mess := ProdMessage{id, stringTime, seq, payload}
	return mess
}