package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"log"
	adapter "worker-template/adapter/kafka"
	"worker-template/model"
)

type alertWorker struct {
	ctx  context.Context
	name string
	conf *model.Config
}

func NewAlertWorker(ctx context.Context, conf *model.Config) Processor {
	return &alertWorker{
		ctx:  ctx,
		name: "",
		conf: conf,
	}
}
func handleFunc(_ string, _ []byte, value []byte) error {
	var event model.Event
	if err := json.Unmarshal(value, &event); err != nil {
		return err
	}
	log.Printf("Event: %v", event)

	return nil
}

func (w *alertWorker) Start() {
	fmt.Println("alert start")
	group, ctx := errgroup.WithContext(w.ctx)
	group.Go(func() error {
		consumer, err := adapter.NewConsumer(ctx, w.conf.Adapter.Kafka.Consumer, handleFunc)
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
	fmt.Println("end ")
	if err := group.Wait(); err != nil {
		log.Printf("One or more groutine error, err: %v", err)
		return
	}
}
