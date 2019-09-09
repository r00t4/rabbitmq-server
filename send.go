package main

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type RabbitMQSend struct {
	conn             *amqp.Connection
	ch               *amqp.Channel
	q                amqp.Queue
	Msgs             <-chan amqp.Delivery
	sendedCorrId     []string
	received         map[string]string
	receivedMapMutex sync.RWMutex
	stop             chan bool
}

func NewRabbitMQSend(url string) RabbitMQSend {
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		"rpc_queue", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")
	stop := make(chan bool)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var received = make(map[string]string)

	var someMapMutex = sync.RWMutex{}
	var str []string
	return RabbitMQSend{conn, ch, q, msgs, str, received, someMapMutex, stop}
}

func (r *RabbitMQSend) CloseRabbitMQ() {
	r.stop <- true
	r.conn.Close()
	r.ch.Close()
}

func (r *RabbitMQSend) Listen() {
	defer r.conn.Close()
	defer r.ch.Close()

	go func() {
		for d := range r.Msgs {
			r.receivedMapMutex.Lock()
			r.received[string(d.CorrelationId)] = string(d.Body)
			r.receivedMapMutex.Unlock()
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting RPC requests")
	<-r.stop
}

func (r *RabbitMQSend) send(n int, corrId string) (res int, err error) {

	err = r.ch.Publish(
		"",          // exchange
		"rpc_queue", // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: string(corrId),
			ReplyTo:       r.q.Name,
			Body:          []byte(strconv.Itoa(n)),
		})
	failOnError(err, "Failed to publish a message")

	//r.sendedCorrId <- string(corrId)

	return
}

func (r *RabbitMQSend) receive(id string, str chan string) {

	r.receivedMapMutex.Lock()
	res := r.received[id]
	r.receivedMapMutex.Unlock()
	for res == "" {
		r.receivedMapMutex.Lock()
		res = r.received[id]
		r.receivedMapMutex.Unlock()
	}
	str <- res

}

func main() {
	r := NewRabbitMQSend("amqp://guest:guest@localhost:5672/")
	go r.Listen()
	k := make(chan string)
	corrId, err := exec.Command("uuidgen").Output()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s", corrId)
	r.send(2, string(corrId))
	r.send(2, string(corrId))
	//go r.receive()
	go r.receive(string(corrId), k)
	select {
	case p := <-k:
		fmt.Println("p: ", p)
	case <-time.After(3 * time.Second):
		fmt.Println(string(corrId), r.received)
		fmt.Println("timeout after 3 seconds")
	}

	r.CloseRabbitMQ()
}
