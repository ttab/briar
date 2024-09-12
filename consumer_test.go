package briar_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
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

const testDeliveryLimit = 5

func TestConsumer(t *testing.T) {
	fixedSeed := os.Getenv("FIXED_SEED") == "true"

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

	targetCount := 40
	routingKeys := []string{"rA", "rB"}
	prepChan := make(chan producedSpec)

	var m sync.Mutex

	state := make(map[string]*recieverState)

	var r *rand.Rand

	if fixedSeed {
		r = rand.New(rand.NewPCG(42, 42))
	} else {
		t := time.Now()
		r = rand.New(rand.NewPCG(uint64(t.UnixMicro()), uint64(t.UnixMilli())))
	}

	go dummyProducer(t, r, conn, exName, targetCount, routingKeys, prepChan)

	var handleN int

	go func() {
		for expect := range prepChan {
			m.Lock()

			handleN += min(expect.FailN, testDeliveryLimit)
			handleN += expect.RequeueN

			if expect.Deadletter {
				handleN++
			}

			if !expect.Deadletter && expect.FailN < testDeliveryLimit {
				// Count the final ack
				handleN++
			}

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
			ExchangeName:      exName,
			QueueName:         qName,
			DeliveryLimit:     testDeliveryLimit,
			RetryHandlingWait: 1 * time.Millisecond,
			OnError: func(code briar.TransitionCode, count int, err error) {
				println(code, count, err.Error())
			},
		},
		briar.AckHandler(func(
			ctx context.Context, delivery amqp.Delivery,
		) (act briar.Action, _ error) {
			handled++

			// Stop the consumer when we have handled the expected
			// number of messages.
			if handled == handleN {
				defer func() {
					// Allow some time to detect if we fetch
					// more than expected.
					time.Sleep(1 * time.Second)
					cancelConsumer()
				}()
			}

			var spec producedSpec

			err := json.Unmarshal(delivery.Body, &spec)
			if err != nil {
				logger.Warn("got malformed message", "err", err)
				return briar.Ack, briar.NewDeadletterError()
			}

			m.Lock()
			st, ok := state[spec.Key]

			defer func() {
				state[spec.Key] = st
				m.Unlock()
			}()

			if !ok {
				t.Errorf("got unknown message %q", delivery.MessageId)

				return briar.ManualAck, briar.NewDeadletterError()
			}

			switch {
			case spec.Deadletter:
				st.DeadletteredN++

				return briar.ManualAck, briar.NewDeadletterError()
			case spec.FailN > st.FailedN:
				st.FailedN++

				return briar.NackDiscard, nil
			case spec.RequeueN > st.RequeuedN:
				st.RequeuedN++

				return briar.NackRequeue, nil
			default:
				st.AckedN++

				return briar.Ack, nil
			}
		}))

	err = consumer.Run(consumerCtx)
	itest.Must(t, err, "run consumer")

	if handled != handleN {
		t.Errorf("unexpected recieve count: got %d, expected %d",
			handled, handleN)
	}

	for k, s := range state {
		t.Run(k, func(t *testing.T) {
			if s.AckedN > 1 {
				t.Fatal("acked more than once")
			}

			expectAck := 1
			expectFails := min(testDeliveryLimit, s.Spec.FailN)
			expectRequeue := s.Spec.RequeueN

			var expectDeadletter int

			if s.Spec.Deadletter {
				expectDeadletter = 1
				expectAck = 0
				expectFails = 0
				expectRequeue = 0
			}

			if s.Spec.FailN >= testDeliveryLimit {
				expectAck = 0
			}

			if s.AckedN != expectAck {
				t.Fatalf("expected %d acks, got %d",
					expectAck, s.AckedN)
			}

			if s.FailedN != expectFails {
				t.Fatalf("expected %d failures, got %d",
					expectFails, s.FailedN)
			}

			if s.RequeuedN != expectRequeue {
				t.Fatalf("expected %d requeues, got %d",
					expectRequeue, s.RequeuedN)
			}

			if s.DeadletteredN != expectDeadletter {
				t.Fatalf("expected %d deadlettering, got %d",
					expectDeadletter, s.DeadletteredN)
			}
		})
	}

	for _, k := range slices.Sorted(maps.Keys(state)) {
		s := state[k]

		fmt.Printf("%s a: %02d f: %02d (%02d) r: %02d (%02d) d: %02d (%v)\n",
			k, s.AckedN,
			s.FailedN, s.Spec.FailN,
			s.RequeuedN, s.Spec.RequeueN,
			s.DeadletteredN, s.Spec.Deadletter)
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
	t *testing.T, r *rand.Rand, conn *amqp.Connection, ex string,
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

		switch r.IntN(3) {
		case 0:
			spec.FailN = sent % 7
		case 1:
			spec.RequeueN = sent % 7
		case 2:
			spec.Deadletter = true
		}

		spec.Headers["dummy-value"] = r.Int()

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
