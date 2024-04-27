package processor

import (
	"context"
	"log"
	"worker-template/model"
)

type Processor interface {
	Start()
}

type Factory func(context.Context, *model.Config) Processor

var processorFunc = map[string]Factory{
	"alert":   NewAlertWorker,
	"event":   NewEventWorker,
	"expried": NewExpriedWorker,
}

func NewProcessor(ctx context.Context, name string, conf *model.Config) Processor {
	fn, ok := processorFunc[name]
	if !ok {
		log.Fatalf("Processor %v doesn't register!\n", name)
	}
	processor := fn(ctx, conf)
	return processor
}
