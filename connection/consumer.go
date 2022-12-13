package connection

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
)

func CommentConsumer() {
	topic := "comments"
    worker, err := ConnectConsumer([]string{"localhost:9092"})
    if err != nil {
        panic(err)
    }
    // calling ConsumePartition. It will open one connection per broker
    // and share it for all partitions that live on it.
    consumer, err := worker.ConsumePartition(topic, 0,sarama.OffsetOldest)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Consumer started ")
	// read the consumer messages using channel 
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Count how many message processed
	msgCount := 0

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}
}