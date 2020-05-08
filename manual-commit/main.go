package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	var wg sync.WaitGroup
	wg.Add(2)

	done := make(chan struct{})
	go func() {
		if err := runListener(done); err != nil {
			fmt.Printf("listener error: %v\n", err)
		}
		wg.Done()
	}()

	go func() {
		err := runConsumer(done)
		fmt.Printf("consumer error: %v\n", err)
		wg.Done()
	}()

	// shutdown hook that signals to the consumers to perform a clean shutdown
	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
		<-sigchan
		close(done)
	}()

	wg.Wait()
}

func runConsumer(done <-chan struct{}) error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": "localhost",
		"group.id":           "cow",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})

	if err != nil {
		return err
	}

	if err = c.SubscribeTopics([]string{"test"}, nil); err != nil {
		return err
	}

	for {
		select {
		case <-done:
			return nil
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				_, err := c.CommitMessage(e)
				if err != nil {
					return fmt.Errorf("committing offset: %w", err)
				}
			case kafka.Error:
				// Errors should generally be considered informational, the client will try to
				// automatically recover. But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					return e
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
}

func runListener(done <-chan struct{}) error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"compression.codec": "snappy", "bootstrap.servers": "localhost"})
	if err != nil {
		return err
	}

	topic := "test"
	h := http.Server{
		Addr: ":8090",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			msg := r.URL.Query().Get("msg")
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(msg),
			}, nil)
		}),
	}

	go func() {
		<-done
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		h.Shutdown(ctx)

	}()
	return h.ListenAndServe()
}
