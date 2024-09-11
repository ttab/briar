package briar_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ttab/briar"
	"github.com/ttab/briar/internal/itest"
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
		}, func(_ context.Context, delivery amqp.Delivery) error {
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

func TestConsumer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(itest.Context(t))
	defer cancel()

	bs := itest.SetUpBackingServices(t)

	conn, ch := rabbitConnect(t, bs.RabbitURI)

	exName := t.Name()
	qName := t.Name() + "-cqueue"

	err := ch.ExchangeDeclare(exName, amqp.ExchangeTopic,
		true, false, false, false, nil)
	itest.Must(t, err, "create test exchange")

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	targetCount := 20
	handleN := targetCount
	deliveryLimit := 2
	routingKeys := []string{"rA", "rB"}
	prepChan := make(chan producedSpec)

	var m sync.Mutex

	state := make(map[string]*recieverState)

	go dummyProducer(t, conn, exName, targetCount, routingKeys, prepChan)

	go func() {
		for expect := range prepChan {
			println("got spec")

			m.Lock()

			handleN += min(expect.FailN, deliveryLimit)
			handleN += expect.RequeueN

			state[expect.Key] = &recieverState{
				Spec: expect,
			}

			m.Unlock()
		}
	}()

	var handled int

	consumerCtx, cancelConsumer := context.WithTimeout(ctx, 10*time.Second)
	defer cancelConsumer()

	consumer := briar.NewConsumer(
		logger.With("component", "consumer"),
		bs.RabbitURI,
		briar.ConsumerOptions{
			ExchangeName:  exName,
			QueueName:     qName,
			DeliveryLimit: int64(deliveryLimit),
			OnError: func(code briar.TransitionCode, count int, err error) {
				println(code, count, err.Error())
			},
		},
		briar.AckHandler(func(
			ctx context.Context, delivery amqp.Delivery,
		) (briar.Action, error) {
			handled++

			// Stop the consumer when we have handled the expected
			// number of messages.
			if handled == handleN {
				defer cancelConsumer()
			}

			println("id", delivery.MessageId)
			enc := json.NewEncoder(os.Stderr)
			enc.SetIndent("", "  ")
			enc.Encode(delivery.Headers)

			var spec producedSpec

			err := json.Unmarshal(delivery.Body, &spec)
			if err != nil {
				logger.Warn("got malformed message", "err", err)
				return briar.Ack, briar.NewDeadletterError()
			}

			m.Lock()
			st := state[spec.Key]
			m.Unlock()

			println("has state", st.Spec.Key)

			if spec.Deadletter {
				st.DeadletteredN++

				return briar.ManualAck, briar.NewDeadletterError()
			}

			if spec.FailN > st.FailedN {
				st.FailedN++

				return briar.NackDiscard, nil
			}

			if spec.RequeueN > st.RequeuedN {
				st.RequeuedN++

				return briar.NackRequeue, nil
			}

			st.AckedN++

			return briar.Ack, nil
		}))

	err = consumer.Run(consumerCtx)
	itest.Must(t, err, "run consumer")

	for k, s := range state {
		fmt.Printf("%s a: %02d f: %02d (%02d) r: %02d (%02d) d: %02d\n",
			k, s.AckedN,
			s.FailedN, s.Spec.FailN,
			s.RequeuedN, s.Spec.RequeueN,
			s.DeadletteredN)
	}
}

type recieverState struct {
	Spec          producedSpec
	AckedN        int
	FailedN       int
	RequeuedN     int
	DeadletteredN int
}

type producedSpec struct {
	Key        string
	FailN      int
	Deadletter bool
	RequeueN   int
	RoutingKey string
	Headers    amqp.Table
}

func dummyProducer(
	t *testing.T, conn *amqp.Connection, ex string,
	tCount int, rKeys []string, prep chan producedSpec,
) {
	t.Helper()

	time.Sleep(1 * time.Second)

	defer close(prep)

	ch, err := conn.Channel()
	itest.Must(t, err, "create producer channel")

	var sent int

	for sent < tCount {
		rKey := rKeys[sent%len(rKeys)]

		spec := producedSpec{
			Key:        fmt.Sprintf("%s-%02d", t.Name(), sent),
			RoutingKey: rKey,
			Headers:    make(amqp.Table),
		}

		switch rand.IntN(3) {
		case 0:
			spec.FailN = sent % 4
		case 1:
			spec.RequeueN = sent % 5
		case 2:
			spec.Deadletter = true
		}

		spec.Headers["dummy-value"] = rand.Int()

		payload, err := json.Marshal(spec)
		itest.Must(t, err, "marshal message spec payload")

		// Use side channel to let the test construct the wanted end
		// state.
		prep <- spec

		err = ch.Publish(ex, rKey, true, false, amqp.Publishing{
			MessageId:    spec.Key,
			Headers:      spec.Headers,
			ContentType:  "application/json",
			Body:         payload,
			DeliveryMode: amqp.Persistent,
			AppId:        "test-producer",
		})
		itest.Must(t, err, "publish test message")

		println("sent", rKey, spec.Key)

		sent++
	}
}

func rabbitConnect(t *testing.T, uri string) (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial(uri)
	itest.Must(t, err, "connect to rabbit")

	t.Cleanup(func() {
		err := conn.Close()
		itest.Must(t, err, "close rabbit connection")
	})

	ch, err := conn.Channel()
	itest.Must(t, err, "open channel")

	return conn, ch
}
