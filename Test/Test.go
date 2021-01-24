package main

import (
	"EsercizioGo/Broker"
	"EsercizioGo/Consumer"
	"EsercizioGo/Producer"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)



func main() {
	//a simple test that checks the correct delivery of a message
	os.Remove("../resultTest.json")
	go Broker.Run(1)
	time.Sleep(3*time.Second)
	go Producer.Run(1)
	time.Sleep(3*time.Second)
	go Consumer.Run(1)
	time.Sleep(3*time.Second)
	sent := getMessages("../Producer/test.JSON")[0]
	received := getMessages("../resultTest.json")[0]
	if sent.Payload != received.Payload{
		fmt.Fprintf(os.Stderr,"Test not passed\n")
		os.Exit(1)
	}
	fmt.Printf("Test passed\n")
	os.Exit(0)
}

func getMessages(path string) []Producer.Payload {
	//it reads the json file and builds the payloads
	var payloads []Producer.Payload
	var raw []byte
	var er error
	raw, er = ioutil.ReadFile(path)
	if er != nil {
		fmt.Println(er.Error())
		os.Exit(1)
	}
	er = json.Unmarshal(raw, &payloads)
	if er != nil {
		fmt.Println(er.Error())
		os.Exit(1)
	}
	return payloads
}