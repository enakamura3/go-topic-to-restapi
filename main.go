package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/enakamura3/go-topic-to-restapi/models"
)

func main() {

	TOPIC := os.Getenv("TOPIC")
	KAFKAADDRESS := os.Getenv("KAFKA_ADDRESS")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": KAFKAADDRESS, "group.id": "users"})
	if err != nil {
		log.Panic(err)
	}
	defer c.Close()
	log.Println("connected to kafka server")

	// Subscribe to topic
	err = c.SubscribeTopics([]string{TOPIC}, nil)
	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			log.Println("message key:", string(msg.Key))
			log.Println("message body:", string(msg.Value))

			user := models.User{}
			err = json.Unmarshal(msg.Value, &user)
			if err != nil {
				log.Printf("failed to decode JSON at offset %d: %v", msg.TopicPartition.Offset, err)
				continue
			}
			if !doSomething(&user) { // if some error happens, send the message to a retry topic
			}
		}
	}

	log.Printf("closing consumer\n")
	c.Close()
}

func doSomething(user *models.User) bool {
	// add a random function to return false in some calls to force error and send the message to retry topic
	return false
}
