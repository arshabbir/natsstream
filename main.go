package main

import (
	"log"
	"os"
	"runtime"
	"time"

	"github.com/arshabbir/natsstream/natstream"
)

func main() {

	//wait := make(chan int)
	os.Setenv("NATSCLUSTER", "test-cluster")

	stream := natstream.NewMessageBroker()

	if err := stream.Connect(); err != nil {
		log.Println("Error connecting to the event bus")
		return
	}

	log.Println("Connection Successful")

	for i := 0; i < 10; i++ {

		if err := stream.Publish("testsubject", []byte("this is a test message")); err != nil {
			log.Println("Error connecting to the event bus")
			return
		}

		log.Println("Message Sent", i)
	}

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

	//<-wait

	time.Sleep(time.Second * 100)

	runtime.Goexit()
	//return
}
