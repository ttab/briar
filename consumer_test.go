package briar_test

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/ttab/briar"
)

func ExampleConsumer() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c := briar.NewConsumer(logger, "amqp://guest:guest@localhost",
		briar.ConsumerOptions{
			ExchangeName: "test",
			QueueName:    "briar",
		}, func(_ context.Context, delivery amqp091.Delivery) error {
			logger.Info("got message",
				"payload", string(delivery.Body))

			err := delivery.Ack(false)
			if err != nil {
				return fmt.Errorf("ack message: %w", err)
			}

			return nil
		})

	err := c.Run(ctx)
	if err != nil {
		log.Printf("failed to run: %v", err)
	}
}
