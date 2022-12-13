package connection

import (
	"fmt"
	"github.com/Shopify/sarama"
)

func PushCommentToQueue(topic string, message []byte) error {
    brokersUrl := []string{"localhost:9092"}
    producer, err := ConnectProducer(brokersUrl)
    if err != nil {
        return err
    }
    defer producer.Close()
    msg := &sarama.ProducerMessage{
        Topic: topic,
        Value: sarama.StringEncoder(message),
    }
    partition, offset, err := producer.SendMessage(msg)
    if err != nil {
        return err
    }
    fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
    return nil
}