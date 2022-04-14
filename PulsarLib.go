package PulsarLib

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type MessageResponse struct {
	Id  string `json:"id"`
	Msg string `json:"msg"`
}

func NewRequest(msg string) *MessageResponse {
	m := MessageResponse{Msg: msg}
	m.Id = ""
	return &m
}

//Default configuration URL: "pulsar://localhost:6650"

func InitClient(URL string) *pulsar.Client {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               URL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
		Authentication:    pulsar.NewAuthenticationToken("yJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY"),
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	return &client
}

func WaitEvent(client *pulsar.Client, topic string, foo func([]byte)) {
	go func() {
		consumer, err := (*client).Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: topic,
			Type:             pulsar.Exclusive,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer consumer.Close()
		for {
			msg, err := consumer.Receive(context.Background())
			if err != nil {
				log.Fatal(err)
			}

			go foo(msg.Payload())
			consumer.Ack(msg)
		}
		//Destroy consumer
		if err := consumer.Unsubscribe(); err != nil {
			log.Fatal(err)
		}
	}()
}

func SendRequestSync(client *pulsar.Client, topic string, message *MessageResponse) []byte {
	id := rand.Intn(10000)
	topicId := topic + "." + strconv.Itoa(id)
	message.Id = topicId
	msg, _ := json.Marshal(message)
	consumer, err := (*client).Subscribe(pulsar.ConsumerOptions{
		Topic:            topicId,
		SubscriptionName: topicId,
		Type:             pulsar.Exclusive,
	})
	if err != nil {
		log.Fatal(err)
	}

	producer, err := (*client).CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: msg,
	})

	if err != nil {
		fmt.Println("Failed to publish message", err)
	}
	fmt.Println("Published message" + string(msg))

	res, err := consumer.Receive(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	//Destroy consumer
	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
	return res.Payload()
}

func SendRequestAsync(client *pulsar.Client, topic string, message *MessageResponse, foo func([]byte)) {
	id := rand.Intn(10000)
	topicId := topic + "." + strconv.Itoa(id)
	message.Id = topicId
	msg, _ := json.Marshal(message)

	consumer, err := (*client).Subscribe(pulsar.ConsumerOptions{
		Topic:            topicId,
		SubscriptionName: topicId,
		Type:             pulsar.Exclusive,
	})
	if err != nil {
		log.Fatal(err)
	}

	producer, err := (*client).CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: msg,
	})

	if err != nil {
		fmt.Println("Failed to publish message", err)
	}
	fmt.Println("Published message " + string(msg))

	res, err := consumer.Receive(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	//Destroy consumer
	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
	go foo(res.Payload())

}

func SendMessage(client *pulsar.Client, topic string, message []byte) {
	producer, err := (*client).CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: message,
	})

	if err != nil {
		fmt.Println("Failed to publish message", err)
	}
	fmt.Println("Published message" + string(message))
}
