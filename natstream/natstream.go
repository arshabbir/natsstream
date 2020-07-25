package natstream

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/nats-io/stan.go"
	//stan "github.com/nats-io/go-nats-streaming"
)

type eventBus struct {
	scon      stan.Conn
	clusterId string
	clientId  string
	natURL    string
}

type EventBus interface {
	Connect() *error
	Publish(string, []byte) *error
	Subscribe(subject string, handler func([]byte)) *error
}

func NewMessageBroker() EventBus {

	//connStr := os.Getenv("BUSCONNSTR")
	clusterId := os.Getenv("NATSCLUSTER")
	natURL := os.Getenv("NATURL")

	clientId := fmt.Sprintf("client%d", rand.Intn(10000))

	log.Println("NewMessageBroker : ", clientId)

	if clusterId == "" || natURL == "" {
		log.Println("Set the Environment variable  NATSCLUSTER & NATURL")
		os.Exit(1)
	}

	return &eventBus{clientId: clientId, clusterId: clusterId, natURL: natURL}

}

func (e *eventBus) Connect() *error {

	//stan.Connect()
	//ss.Connect()

	sc, err := stan.Connect(
		e.clusterId,
		e.clientId,
		stan.NatsURL(e.natURL),
	)

	if err != nil {
		log.Println("Error connecting to the NATS Stream Server")
		os.Exit(1)
	}

	e.scon = sc

	return nil

}

func (e *eventBus) Publish(subject string, mesg []byte) *error {

	id, err := e.scon.PublishAsync(subject, mesg, func(id string, err error) {
		if err != nil {
			log.Println("Error getting ack for message id : ", id)
		}
		log.Println("Ack received : ", id)
		return
	})

	if err != nil {

		log.Println("Error getting ack for message id : ", id)

	}

	return nil

}

//simple(a func(a, b int) int)
func (e *eventBus) Subscribe(subject string, handler func(data []byte)) *error {

	log.Println("Durable ", e.clientId)

	log.Println("Client Subject ", subject)

	//log.Println("Client cluster ", )

	e.scon.Subscribe(subject, func(mb *stan.Msg) {
		mb.Ack()
		log.Println("*****Invoked")
		handler(mb.Data)

		return
	}, stan.DurableName(e.clientId),
		stan.MaxInflight(25),
		stan.SetManualAckMode(),
		stan.AckWait(time.Second*30))

	return nil

}
