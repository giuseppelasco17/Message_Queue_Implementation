package Producer

import (
	_ "hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var test int //a variable used to enable the test mode

var queueConf QueueConf //the struct in which are loaded the configuration param of the queue

var messages []ProdMessage//list of messages loaded from a json file

type Producer int //used to expose the RPC functions

var IDProd int //the producer ID

type Ack struct { //the ack sent by the consumer to the broker
	Seq int
}

var ackChan chan Ack //the channel that notifies the acks arrival

var wg sync.WaitGroup //used to wait the goroutine to finish

func server(portNum string){
	//installs the server side functionality and registers the functions
	comm := new(Producer)
	err := rpc.Register(comm)
	if err != nil {
		log.Fatal("error registering API", err)
	}
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":" + portNum)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("Port listner: %s\n", portNum)
	err = http.Serve(l, nil)
	if err != nil {
		log.Fatal("error in serve", err)
	}
}

func (t*Producer) ReceiveAck(arg Ack, reply *string) error{
	//used by the broker to forward/send the ack to the producer
	ackChan <- arg
	reply = nil
	return nil
}

func (t *Producer) Error(arg string, reply *string) error {
	//the broker use this function to send error messages to the producer
	println(arg)
	reply = nil
	go func() {
		os.Exit(1)
	}()
	return nil
}

func producer(){
	//used to send messages to the queue
	client, err := rpc.DialHTTP("tcp", "localhost:"+queueConf.BrokerPort)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	if queueConf.Semantic == "at-least"{//at-least-once semantics case
		ackChan = make(chan Ack)
		portNum := queueConf.ProducerPort//it sends its port number to the broker
		err = client.Call("Broker.ProdConnect", portNum, nil)
		if err != nil {
			log.Fatal("Error in Broker.ProdConnect: ", err)
		}
		go server(portNum)//it sets up the server side
		//it sends the messages loaded from a json file
		i := 0
		for i = 0; i < len(messages); i++{
			wg.Add(1)
			arg := messages[i]
			var reply string
			client.Go("Broker.Send", arg, &reply, nil)
			go checkAndRetransmit(arg)//this goroutine manages the retransmission
		}
		wg.Wait()//wait all goroutine finishes
	}else {//timeout-based semantics case
		i := 0
		for i = 0; i < len(messages); i++ {
			arg := messages[i]
			var reply string
			client.Go("Broker.Send", arg, &reply, nil)//sends messages
		}
	}
}

func checkAndRetransmit(message ProdMessage) {
	//it starts a timer when a message is sent, if it expires retransmit the messages, otherwise, if the ack is received
	//stop the timer and the goroutine finishes
	retransmitTimer := time.NewTimer(time.Duration(queueConf.Timeout)* time.Millisecond)
	for true {
		select {
		case <-retransmitTimer.C:
			//println("retransmit:", message.Seq)
			var reply string
			client, err := rpc.DialHTTP("tcp", "localhost:"+queueConf.BrokerPort)
			if err != nil {
				log.Fatal("dialing:", err)
			}
			client.Go("Broker.Send", message, &reply, nil)
			retransmitTimer = time.NewTimer(time.Duration(queueConf.Timeout) * time.Millisecond)
		case ack := <-ackChan:
			if ack.Seq == message.Seq {
				retransmitTimer.Stop()
				//fmt.Printf("ok %d\n", ack.Seq)
				wg.Done()
				return
			}
		}
	}
}

func initialize() {
	//parse the json configuration file
	queueConf = getConfig(test)
	if queueConf.Semantic != "at-least" && queueConf.Semantic != "timeout-based"{
		println("Wrong semantic")
		os.Exit(1)
	}
	messages = getMessages(IDProd, test)
}

func Run(t int) {
	test = t
	createID()
	initialize()
	producer()
}

