package main

import (
	"log"
	"os"
	"time"

	"github.com/arshabbir/natsstream/natstream"
)

func main() {

	os.Setenv("NATSCLUSTER", "test-cluster")

	stream := natstream.NewMessageBroker()

	if err := stream.Connect(); err != nil {
		log.Println("Error connecting to the event bus")
		return
	}

	log.Println("Connection Successful")

	if err := stream.Publish("testsubject", []byte("this is a test message")); err != nil {
		log.Println("Error connecting to the event bus")
		return
	}

	log.Println("Message Sent")

	//Client code

	client := natstream.NewMessageBroker()

	if err := client.Connect(); err != nil {
		log.Println("Error connecting to the event bus")
		return
	}

	log.Println("Client Connection Successful")

	client.Subscribe("testsubject", func(msg []byte) {

		log.Println("Message Received : ")
		log.Println(string(msg))

	})

	time.Sleep(time.Second * 10)

	return
}
