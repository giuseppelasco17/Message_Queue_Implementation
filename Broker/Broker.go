package Broker

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var test int//a variable used to enable the test mode

var messageQueue *Queue //message queue managed by the broker in which it saves temporary the messages

var consumersPorts []string //list of consumers connected to the broker

var producerPort string //the producer port

type Broker int //used to expose the RPC functions

var mutex sync.Mutex //to synchronize the connection closure and the send of messages avoiding to send a message
					 // to a consumer just disconnected

type Message struct { //the message structure that contains all info necessary for the broker
	IDProducer int
	Time       string
	Seq        int
	Visibility int
	Error 	   int
	Text 	   string
}

type ProdMessage struct { //the message structure sent by the producer
	IDProducer int
	Time string
	Seq int
	Payload string
}

var consumersPortsMap map[int]string //a map {[sequence number]consumer port } necessary to retransmit a message towards
									 //the correct consumer

var visibilityTimer *time.Timer //when it expires the message become visible again

type Ack struct { //the ack sent by the consumer to the broker
	Seq int
}

var consumersConnections = 1 //a variable that tracks the number of consumer connected, to assign them a port number

var wg sync.WaitGroup //it permits to manage the goroutines

var ackChan chan Ack //the channel that notifies the acks arrival

var queueConf QueueConf //the struct in which are loaded the configuration param of the queue

func (t*Broker) Send(arg ProdMessage, rep *string) error{
	//function called by the producer to send messages on the queue
	mess := messageInit(arg)
	messageQueue.Append(&mess)
	rep = nil
	return nil
}

func (t *Broker) ConsConnect(arg string, reply *string) error {
	//called by the consumer to connect to the queue; the function returns to the consumer the number port to use
	brokerPort , _ := strconv.Atoi(queueConf.BrokerPort)
	portInt := brokerPort + consumersConnections
	port := strconv.Itoa(portInt)
	consumersPorts = append(consumersPorts, port)
	consumersConnections++
	*reply = port
	return nil
}

func (t *Broker) ProdConnect(port string, reply *string) error {
	//called by the producer to connect to the queue; it provides hts number port
	consumersPortsMap = make(map[int]string)
	producerPort = port
	reply = nil
	return nil
}

func (t* Broker) RetrieveMessage(arg string, reply *Message) error{
	//called by the consumer to retrieve a message from the queue in the timeout-based delivery case
	clientSideTimeoutBased(reply)
	return nil
}

func (t*  Broker) SendAck(arg Ack, reply *string) error{
	//used by the consumer to send ack to the producer through the queue
	ackChan <- arg
	*reply = ""
	return nil
}

func (t* Broker) CloseConnection(arg string, reply *string) error{
	//called by the consumer to close its connection; remove the port from the consumersPorts
	go func() {//a go routine do it to return immediately the call
		mutex.Lock()
		*reply = ""
		var cons []string
		for _, v := range consumersPorts{
			if v != arg{
				cons = append(cons, v)
			}
		}
		consumersPorts = cons
		mutex.Unlock()
	}()
	return nil
}

func server(){
	//installs the server side functionality and registers the functions
	comm := new(Broker)
	err := rpc.Register(comm)
	if err != nil {
		log.Fatal("error registering API", err)
	}
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+queueConf.BrokerPort)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("Port listner: %s\n", queueConf.BrokerPort)
	err = http.Serve(l, nil)
	if err != nil {
		log.Fatal("error in serve", err)
	}
}

func forwardAck(){
	//it sends the ack to the producer. retrieving it from the channel filled by the SendAck function
	for true {
		var arg Ack
		arg = <-ackChan
		ack := Ack{arg.Seq}
		var reply string
		client, err := rpc.DialHTTP("tcp", "localhost:"+producerPort)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		client.Go("Producer.ReceiveAck", ack, &reply, nil)
	}
}

func clientSideAtLeast() {
	//this function sends the messages saved in the queue to the consumers
	i := 0
	for true {
		arg := messageQueue.Pop()//retrieves the message from the queue
		mutex.Lock()
		if len(consumersPorts) == 0{//checks that there is at least a consumer connected, otherwise sends an error to
									//the consumer
			client, err := rpc.DialHTTP("tcp", "localhost:"+producerPort)
			if err != nil {
				log.Fatal("dialing:", err)
			}
			var reply string
			arg := "there are no consumers"
			err = client.Call("Producer.Error", arg, &reply)
			if err != nil {
				log.Fatal("SendMessage error:", err)
			}
			println("No consumers")
			mutex.Unlock()
			os.Exit(1)
		}
		v,ok := consumersPortsMap[arg.Seq]
		var consumerPort string
		if !ok {//it checks if it sees the message the first time, if no sends the message to a consumer using round
				//robin implementing a load balance like forwarding
			consumerPort = consumersPorts[i%len(consumersPorts)]
			consumersPortsMap[arg.Seq] = consumerPort//assigns the consumer port to the sequence number, to retransmit a
			// message towards the correct consumer
		}else {//if it has already seen the message it send it to the right consumer consulting the consumersPortsMap
			consumerPort = v
		}
		//it sends the message to the consumer
		var reply string
		client, err := rpc.DialHTTP("tcp", "localhost:"+consumerPort)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		client.Go("Consumer.SendMessage", arg, &reply, nil)
		i++
		mutex.Unlock()
	}

}



func clientSideTimeoutBased(reply *Message) {
	//when a client retrieves a message from the queue, in timeout-based semantic case, this function checks if there
	//are messages in the queue or sends the head if it is visible
	if messageQueue.Length() == 0{
		*reply = Message{
			Error: 1,
			Text: "No message in queue",
		}
		return
	}
	message := messageQueue.Head()
	if message.Visibility == 0{
		*reply = Message{
			Error: 1,
			Text: "Message invisible",
		}
		return
	}
	*reply = *message
	message.Visibility = 0
	wg.Add(2)
	go receiveAck(message)
	visibilityTimer = time.NewTimer(time.Duration(queueConf.Timeout)* time.Millisecond)
	go visibilityHandler(message)
}

func receiveAck(message *Message) {
	//a goroutine that checks if the ack is received, than it deletes it from the queue
	ack := <-ackChan
	if message.Seq == -1 {
		wg.Done()
		return
	}
	if ack.Seq == message.Seq {
		messageQueue.Pop()
		message.Seq = -1
		wg.Done()
	}
}

func visibilityHandler(message *Message) {
	//when the visibility timer expires the message becomes visible again
	<- visibilityTimer.C
	message.Visibility = 1
	wg.Done()

}

func initialize() {
	//parse the json configuration file
	queueConf = getConfig(test)
	if queueConf.Semantic != "at-least" && queueConf.Semantic != "timeout-based"{
		println("Wrong semantic")
		os.Exit(1)
	}
}

func queueStatus(){
	//a goroutine that interacts with the user to print the status of the queue
	for true {
		print("Press enter key to print the status report\n")
		var x int
		_,_ = fmt.Scanln(&x)
		length := messageQueue.Length()
		println("# messages in queue: ",length)
		print("List of messages and status: ")
		messageQueue.PrintElem()
	}
}

func Run(t int)  {
	//this function sets up the broker according to the semantics
	test = t
	initialize()
	if queueConf.Semantic == "at-least" {
		messageQueue = newQueue()
		ackChan = make(chan Ack)
		go forwardAck()
		go clientSideAtLeast()
		go server()
		queueStatus()
	}else {
		messageQueue = newQueue()
		ackChan = make(chan Ack)
		go server()
		queueStatus()
	}
}




