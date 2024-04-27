package processor

import (
	"context"
	"fmt"
	"github.com/robfig/cron/v3"
	"worker-template/model"
)

type expriedWorker struct {
	name   string
	ctx    context.Context
	config *model.Config
	cron   *cron.Cron
}

func NewExpriedWorker(ctx context.Context, conf *model.Config) Processor {
	customParser := cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)

	c := cron.New(cron.WithParser(customParser))
	return &expriedWorker{
		name:   "WORKER EXPRIED",
		ctx:    ctx,
		config: conf,
		cron:   c,
	}
}
func (w *expriedWorker) Start() {
	fmt.Println("Expried run")

	w.cron.AddFunc("50 08 19 * * *", func() {
		fmt.Println("Task executed at 9:15:50 PM every day")
	})

	w.cron.Start()
}
