package Consumer

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
)

type Message struct {//the message structure
	IDProducer int
	Time string
	Seq int
	Visibility int
	Error int
	Text string
}

var test int//a variable used to enable the test mode

var queueConf QueueConf //the struct in which are loaded the configuration param of the queue

type Ack struct {//the ack sent by the consumer to the broker
	Seq int
}

var port string//the consumer port assigned by the broker

type Consumer int//used to expose the RPC functions



func consumer() {
	//it asks the port to the broker, create a connection with it and set up an handler to close the connection when the
	//consumer is killed
	client, err := rpc.DialHTTP("tcp", "localhost:"+queueConf.BrokerPort)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	setupCloseHandler(client)
	err = client.Call("Broker.ConsConnect", "", &port)
	if err != nil {
		log.Fatal("Broker.ConsConnect:", err)
	}
	println("connected")
}

func (t*Consumer) SendMessage(arg Message, reply *string) error{
	//the broker calls this function to send the message and sends an ack of it through the queue (at-least-once
	//semantics case)
	println(arg.IDProducer, arg.Text)
	if test != 0{
		marshalMessage(arg)
	}
	ack := Ack{arg.Seq}
	go func() {
		client, err := rpc.DialHTTP("tcp", "localhost:"+queueConf.BrokerPort)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		var rep string
		client.Go("Broker.SendAck", ack, &rep, nil)
	}()
	reply = nil
	return nil
}

func receiveMessage() {
	//it sets up the server side to receive the messages from the broker
	comm := new(Consumer)
	err := rpc.Register(comm)
	if err != nil {
		log.Fatal("error registering API", err)
	}
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "localhost:"+port)
	if e != nil {//if a bind error occurs, it close the connection
		var reply string
		client, err := rpc.DialHTTP("tcp", "localhost:"+queueConf.BrokerPort)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		err = client.Call("Broker.CloseConnection", port, &reply)
		if err != nil {
			log.Fatal("Broker.CloseConnection:", err)
		}
		log.Fatal("listen error:", e)
	}
	log.Printf("Port listner: %s\n", port)
	err = http.Serve(l, nil)
	if err != nil {
		log.Fatal("error in serve", err)
	}
}

func retrieveMessage() {
	//used to retrieve a message and send ack in the timeout-based semantics case
	client, err := rpc.DialHTTP("tcp", "localhost:"+queueConf.BrokerPort)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	var reply Message
	err = client.Call("Broker.RetrieveMessage", "", &reply)
	if err != nil {
		log.Fatal("Broker.RetrieveMessage:", err)
	}
	println(reply.IDProducer,reply.Text)
	if test != 0{
		marshalMessage(reply)
	}
	if reply.Error != 1{
		ack := Ack{reply.Seq}
		client, err = rpc.DialHTTP("tcp", "localhost:"+queueConf.BrokerPort)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		var rep string
		err = client.Call("Broker.SendAck", ack, &rep)
		if err != nil {
			log.Fatal("Broker.SendAck:", err)
		}
	}
}

func setupCloseHandler(client *rpc.Client) {
	//Ctrl+C signal handler to close connection on the consumer kill
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\r- Ctrl+C pressed in Terminal")
		var reply string
		err := client.Call("Broker.CloseConnection", port, &reply)
		if err != nil {
			log.Fatal("Broker.CloseConnection:", err)
		}
		os.Exit(0)
	}()
}

func initialize() {
	//parse the json configuration file
	queueConf = getConfig(test)
	if queueConf.Semantic != "at-least" && queueConf.Semantic != "timeout-based"{
		println("Wrong semantic")
		os.Exit(1)
	}
}

func Run(t int) {
	test = t
	initialize()
	if queueConf.Semantic == "at-least"{
		consumer()
		receiveMessage()
	}else {
		retrieveMessage()
	}
}


