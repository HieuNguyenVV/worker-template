package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"log"
	"time"
	adapter "worker-template/adapter/kafka"
	"worker-template/model"
)

type eventWorker struct {
	name     string
	ctx      context.Context
	conf     *model.Config
	producer adapter.Producer
}

func NewEventWorker(ctx context.Context, conf *model.Config) Processor {
	pro, err := adapter.NewProducer(ctx, conf.Adapter.Kafka.Producer)
	if err != nil {
		log.Fatalf("Error connect to kafka, err: %v", err)
	}
	return &eventWorker{
		name:     "WORKER EVENT",
		ctx:      ctx,
		conf:     conf,
		producer: pro,
	}
}

func (w *eventWorker) Start() {
	fmt.Println("event start")
	group, ctx := errgroup.WithContext(w.ctx)

	group.Go(func() error {
		con := w.conf.Adapter.Kafka.Consumer
		con.Topics = w.conf.Adapter.Kafka.Topic.ConsumerEvent
		consumer, err := adapter.NewConsumer(ctx, con, w.handleFunc)
		if err != nil {
			log.Printf("Error create consumer, err: %v", err)
			return err
		}
		defer consumer.Close()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				continue
			}
		}
	})
	if err := group.Wait(); err != nil {
		log.Printf("One or more groutine error, err: %v", err)
		return
	}
}
func (w *eventWorker) handleFunc(_ string, _ []byte, value []byte) error {
	var event model.Event
	if err := json.Unmarshal(value, &event); err != nil {
		return err
	}
	log.Printf("Event: %v", event)
	for i := 0; i <= 100; i++ {
		event := model.Event{
			Msg:      fmt.Sprintf("Event: %v", i),
			CreateAt: time.Now().Unix(),
		}

		body, err := json.Marshal(&event)
		if err != nil {
			return err
		}
		if err := w.producer.Producer(w.conf.Adapter.Kafka.Topic.TopicEvent, "", body); err != nil {
			log.Printf("Producer msg error, err: %v", err)
			return err
		}
	}
	return nil
}
