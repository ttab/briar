package briar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	consumerClosed = iota
	consumerLostConnection
	consumerCancelled
	consumerConnected
	consumerLostChannel
	consumerHasChannel
	consumerDeclared
	consumerConsuming
)

type (
	HaltFunc  func(code TransitionCode, count int, err error) bool
	ErrorFunc func(code TransitionCode, count int, err error)
)

type ConsumerOptions struct {
	ConnectionName string
	ExchangeName   string
	QueueName      string
	// DeliveryLimit is the number of times we'll try to handle a message
	// before deadlettering it.
	DeliveryLimit int64
	// RetryHandlingWait is the time we wait before retrying a failed message.
	RetryHandlingWait time.Duration
	RoutingKeys       []string
	Exclusive         bool
	RetryWait         time.Duration
	ConnectRetryWait  time.Duration
	ChannelRetryWait  time.Duration
	HaltFunction      HaltFunc
	OnError           ErrorFunc
}

type Consumer struct {
	logger  *slog.Logger
	opts    ConsumerOptions
	handler HandlerFunc

	uri              string
	connectionName   string
	exchangeName     string
	queueName        string
	deadExchangeName string
	// The dead queue is where messages end up when we've given up on
	// processing them.
	deadQueueName string
	routingKeys   []string
	exclusive     bool

	state int
	evt   chan int

	conn *amqp.Connection
	ch   *amqp.Channel

	connClosed chan *amqp.Error
	chanCancel chan string
	chanClose  chan *amqp.Error
	deliveries <-chan amqp.Delivery
}

type HandlerFunc func(ctx context.Context, delivery amqp.Delivery) error

type Action int

const (
	// Ack default ack this msg after you have successfully processed this
	// delivery.
	Ack Action = iota
	// NackDiscard the message will be dropped or delivered to a server
	// configured dead-letter queue.
	NackDiscard
	// NackRequeue deliver this message to a different consumer.
	NackRequeue
	// ManualAck leaves acknowledgement to the user using the msg.Ack()
	// method.
	ManualAck
)

// AckHandler is a convenience wrapper that allows the handler to return the
// Ack/Nack action as a value. Returning an error will cause the channel to be
// closed and re-opened. Return a HaltError to stop the consumer.
func AckHandler(
	fn func(ctx context.Context, delivery amqp.Delivery) (Action, error),
) HandlerFunc {
	return func(ctx context.Context, delivery amqp.Delivery) error {
		action, innerErr := fn(ctx, delivery)

		switch action {
		case Ack:
			err := delivery.Ack(false)
			if err != nil {
				return errors.Join(innerErr,
					fmt.Errorf("ack message: %w", err))
			}
		case NackDiscard:
			err := delivery.Nack(false, false)
			if err != nil {
				return errors.Join(innerErr,
					fmt.Errorf("nack message: %w", err))
			}
		case NackRequeue:
			err := delivery.Nack(false, true)
			if err != nil {
				return errors.Join(innerErr,
					fmt.Errorf("requeue message: %w", err))
			}
		case ManualAck:
		default:
			return errors.Join(innerErr,
				errors.New("unknown action"))
		}

		if innerErr != nil {
			return innerErr
		}

		return nil
	}
}

// NewConsumer creates a rabbitmq consumer that is started by calling Run().
func NewConsumer(
	logger *slog.Logger, uri string,
	opts ConsumerOptions,
	handler HandlerFunc,
) *Consumer {
	if opts.RetryWait == 0 {
		opts.RetryWait = 5 * time.Second
	}

	if opts.DeliveryLimit == 0 {
		opts.DeliveryLimit = 5
	}

	if opts.RetryHandlingWait == 0 {
		opts.RetryHandlingWait = 5 * time.Second
	}

	if opts.ConnectRetryWait == 0 {
		opts.ConnectRetryWait = firstNonZeroDuration(
			opts.ConnectRetryWait,
			opts.RetryWait,
		)
	}

	if opts.ChannelRetryWait == 0 {
		opts.ChannelRetryWait = firstNonZeroDuration(
			opts.ChannelRetryWait,
			opts.RetryWait,
		)
	}

	c := Consumer{
		logger:         logger,
		uri:            uri,
		handler:        handler,
		connectionName: opts.ConnectionName,
		exchangeName:   opts.ExchangeName,
		queueName:      opts.QueueName,
		routingKeys:    opts.RoutingKeys,
		exclusive:      opts.Exclusive,
		opts:           opts,
		state:          consumerClosed,
		evt:            make(chan int),
	}

	return &c
}

type jiggleAction string

const (
	actNone       jiggleAction = ""
	actReconnect  jiggleAction = "reconnect"
	actNewChannel jiggleAction = "new_channel"
)

// Run starts the consumer process. This will set up the topology and consume
// messages until:
//
// * the context is cancelled
// * the HaltFunction returns true in response to an error
// * ...or the handler returns an HaltError.
func (c *Consumer) Run(ctx context.Context) error {
	var (
		counts    map[TransitionCode]int
		cancelErr error
	)

	for {
		var tError *transitionError

		select {
		case <-ctx.Done():
			cancelErr = nil
			c.state = consumerCancelled
		default:
		}

		switch c.state {
		case consumerClosed:
			err := c.connect()
			if err != nil {
				tError = tErrorw(CodeConnect, err)

				break
			}

			c.logger.Debug("connected to rabbitmq")

			c.state = consumerConnected
		case consumerLostConnection:
			c.conn = nil
			c.connClosed = nil
			c.ch = nil
			c.chanCancel = nil
			c.chanClose = nil

			c.state = consumerClosed
		case consumerCancelled:
			if c.conn != nil {
				c.logger.Debug("closing rabbitmq connection")

				err := c.conn.Close()
				if err != nil {
					c.logger.Warn(
						"failed to close rabbitmq connection",
						"err", err)
				}
			}

			if cancelErr != nil {
				return cancelErr
			}

			return nil
		case consumerConnected:
			err := c.openChan()
			if err != nil {
				tError = tErrorw(CodeOpenChan, err)

				break
			}

			c.state = consumerHasChannel
		case consumerLostChannel:
			c.ch = nil
			c.chanCancel = nil
			c.chanClose = nil

			c.state = consumerConnected
		case consumerHasChannel:
			err := c.declareAndBind()
			if err != nil {
				tError = tErrorw(CodeDeclare, err)

				break
			}

			c.state = consumerDeclared
		case consumerDeclared:
			err := c.consume()
			if err != nil {
				tError = tErrorw(CodeConsume, err)

				break
			}

			c.logger.Debug("consuming queue")

			c.state = consumerConsuming
		case consumerConsuming:
			tError = c.handle(ctx)
		}

		if tError == nil {
			if counts != nil {
				counts = nil
			}

			continue
		}

		var he *HaltError

		// Check if we hit a halting error.
		if errors.As(tError, &he) {
			c.state = consumerCancelled
			cancelErr = fmt.Errorf("halting retries: %w", tError)

			continue
		}

		if counts == nil {
			counts = make(map[TransitionCode]int)
		}

		counts[tError.code]++

		if c.opts.HaltFunction != nil {
			halt := c.opts.HaltFunction(tError.code,
				counts[tError.code], tError)
			if halt {
				c.state = consumerCancelled
				cancelErr = fmt.Errorf("halting retries: %w", tError)

				continue
			}
		}

		if c.opts.OnError != nil {
			c.opts.OnError(tError.code, counts[tError.code], tError)
		}

		var (
			act   jiggleAction
			sleep time.Duration
		)

		if counts[tError.code] == 3 {
			if tError.code == CodeOpenChan {
				act = actReconnect
			} else {
				act = actNewChannel
			}
		}

		switch tError.code {
		case CodeConnect:
			c.logger.Error("failed to connect",
				"err", tError)

			sleep = c.opts.ConnectRetryWait
			act = actNone
		case CodeOpenChan:
			c.logger.Error("failed to open channel",
				"err", tError)

			sleep = c.opts.ChannelRetryWait
		case CodeDeclare:
			c.logger.Error("failed to declare topology",
				"err", tError)

			sleep = c.opts.RetryWait
		case CodeConsume:
			var rerr *amqp.Error

			switch {
			// Access refused - exclusive use, not an error
			case errors.As(tError, &rerr) && rerr.Code == 403:
				sleep = c.opts.RetryWait * 2
			default:
				c.logger.Error("failed to start consuming",
					"err", tError)

				sleep = c.opts.RetryWait
			}

			act = actNewChannel
		case CodeDisconnected:
			c.logger.Error("disconnected",
				"err", tError)

			c.state = consumerLostConnection
			act = actNone
		case CodeChanClosed:
			c.logger.Error("channel closed",
				"err", tError)

			c.state = consumerLostChannel
			act = actNone
		case CodeHandlerFail:
			c.logger.Error("failed to handle message",
				"err", tError)

			act = actNewChannel
			sleep = c.opts.RetryWait
		case CodeCancelled:
			c.state = consumerCancelled
			act = actNone
		}

		switch act {
		case actNone:
		case actReconnect:
			err := c.conn.Close()
			if err != nil {
				c.logger.Warn(
					"failed to close rabbitmq connection",
					"err", err)
			}

			c.state = consumerLostConnection
		case actNewChannel:
			err := c.ch.Close()
			if err != nil {
				c.logger.Warn(
					"failed to close rabbitmq channel",
					"err", err)
			}

			c.state = consumerLostChannel
		}

		c.sleep(ctx, sleep)
	}
}

func (c *Consumer) sleep(ctx context.Context, wait time.Duration) {
	if wait == 0 {
		return
	}

	select {
	case <-ctx.Done():
	case <-time.After(wait):
	}
}

func (c *Consumer) handle(ctx context.Context) *transitionError {
	select {
	case err := <-c.connClosed:
		return tErrorw(CodeDisconnected, err)
	case tag := <-c.chanCancel:
		return tErrorf(CodeChanClosed, "channel cancelled, tag: %s", tag)
	case err := <-c.chanClose:
		return tErrorw(CodeChanClosed, err)
	case <-ctx.Done():
		return tErrorf(CodeCancelled, "context cancelled")
	case d := <-c.deliveries:
		err := c.deliver(ctx, d)
		if err != nil {
			return tErrorw(CodeHandlerFail, err)
		}
	}

	return nil
}

func (c *Consumer) deliver(
	ctx context.Context, delivery amqp.Delivery,
) (outErr error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer func() {
		p := recover()
		if p != nil {
			buf := make([]byte, 1024)
			n := runtime.Stack(buf, false)

			buf = buf[0:n]

			c.logger.Error("handler panic",
				"err", p,
				"trace", string(buf))

			err := c.deadletterDelivery(ctx, delivery)
			if err != nil {
				outErr = fmt.Errorf(
					"deadletter after delivery limit: %w", err)
			}

			outErr = errors.Join(
				outErr,
				fmt.Errorf("handler panicked: %v", p),
			)
		}
	}()

	if getDeathCount(delivery.Headers, c.queueName) >= c.opts.DeliveryLimit {
		err := c.deadletterDelivery(ctx, delivery)
		if err != nil {
			return fmt.Errorf("deadletter after delivery limit: %w", err)
		}

		return nil
	}

	var de *DeadletterError

	err := c.handler(subCtx, delivery)
	if errors.As(err, &de) {
		err := c.deadletterDelivery(ctx, delivery)
		if err != nil {
			return fmt.Errorf("deadletter after DeadletterError: %w", err)
		}
	} else if err != nil {
		return err
	}

	return nil
}

func (c *Consumer) deadletterDelivery(
	ctx context.Context, delivery amqp.Delivery,
) error {
	newHead := make(amqp.Table, len(delivery.Headers))

	// Create a copy of the old headers with x-death as old-death,
	// that way we retain death metadata, but can easily re-queue
	// failed messages without them immediately failing again.
	for k, v := range delivery.Headers {
		if k == "x-death" {
			k = "old-death"
		}

		newHead[k] = v
	}

	// If a delivery is immediately deadlettered on first delivery
	// by using NewDeadletterError(), replicate the header structure
	// that rabbit uses when deadlettering.
	_, exists := newHead["old-death"]
	if !exists {
		newHead["old-death"] = []interface{}{
			amqp.Table{
				"count":        1,
				"exchange":     c.exchangeName,
				"queue":        c.queueName,
				"reason":       "rejected",
				"routing-keys": []interface{}{delivery.RoutingKey},
				"time":         time.Now(),
			},
		}
		newHead["x-first-death-exchange"] = c.exchangeName
		newHead["x-first-death-queue"] = c.queueName
		newHead["x-first-death-reason"] = "rejected"
	}

	err := c.ch.PublishWithContext(ctx, c.deadExchangeName, routeFail,
		false, false, amqp.Publishing{
			Headers:         newHead,
			ContentType:     delivery.ContentType,
			ContentEncoding: delivery.ContentEncoding,
			DeliveryMode:    amqp.Persistent,
			CorrelationId:   delivery.CorrelationId,
			ReplyTo:         delivery.ReplyTo,
			Expiration:      delivery.Expiration,
			MessageId:       delivery.MessageId,
			Timestamp:       delivery.Timestamp,
			Type:            delivery.Type,
			UserId:          delivery.UserId,
			AppId:           delivery.AppId,
			Body:            delivery.Body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to deadletter delivery: %w", err)
	}

	err = delivery.Ack(false)
	if err != nil {
		return fmt.Errorf(
			"failed to ack delivery after it was deadlettered: %w",
			err,
		)
	}

	return nil
}

func getDeathCount(headers amqp.Table, queue string) int64 {
	deaths, ok := headers["x-death"].([]interface{})
	if !ok || len(deaths) == 0 {
		return 0
	}

	for i := range deaths {
		death, ok := deaths[i].(amqp.Table)
		if !ok {
			continue
		}

		qn, ok := death["queue"].(string)
		if !ok || qn != queue {
			continue
		}

		count, _ := death["count"].(int64)

		return count
	}

	return 0
}

func (c *Consumer) consume() error {
	consumerNoAutoAck := false

	deliveries, err := c.ch.Consume(c.queueName, "",
		consumerNoAutoAck, c.exclusive, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume queue: %w", err)
	}

	c.deliveries = deliveries

	return nil
}

const (
	routeAll           = "#"
	routeScheduleRetry = "schedule-retry"
	routeDoRetry       = "do-retry"
	routeFail          = "fail-message"
)

func (c *Consumer) declareAndBind() error {
	var (
		isDurable       = true
		noAutodelete    = false
		notExclusive    = false
		notInternal     = false
		awaitReply      = false
		deadletterEx    = c.queueName + "-deadletter"
		deadletterRetry = c.queueName + "-deadletter-retry"
		deadletter      = c.queueName + "-deadletter"
	)

	err := c.ch.ExchangeDeclare(
		deadletterEx, amqp.ExchangeTopic,
		isDurable, noAutodelete, notInternal, awaitReply, nil)
	if err != nil {
		return fmt.Errorf("declare deadletter exchange: %w", err)
	}

	c.deadExchangeName = deadletterEx

	dlRetryQueue, err := c.ch.QueueDeclare(deadletterRetry,
		isDurable, noAutodelete, notExclusive, awaitReply, amqp.Table{
			"x-queue-type": "classic",
			// Deadletter (retry) messages after the configured
			// retry wait duration.
			"x-message-ttl":             int(c.opts.RetryHandlingWait.Milliseconds()),
			"x-dead-letter-exchange":    deadletterEx,
			"x-dead-letter-routing-key": routeDoRetry,
		})
	if err != nil {
		return fmt.Errorf("declare retry dead letter queue: %w", err)
	}

	err = c.ch.QueueBind(
		dlRetryQueue.Name, routeScheduleRetry, deadletterEx, awaitReply, nil)
	if err != nil {
		return fmt.Errorf("bind retry queue to dead letter exchange: %w", err)
	}

	dlQueue, err := c.ch.QueueDeclare(deadletter,
		isDurable, noAutodelete, notExclusive, awaitReply, amqp.Table{
			"x-queue-type": "classic",
			// Purge deadlettered messages after 72h.
			"x-message-ttl": 3600 * 1000 * 72,
		})
	if err != nil {
		return fmt.Errorf("declare dead letter queue: %w", err)
	}

	err = c.ch.QueueBind(
		dlQueue.Name, routeFail, deadletterEx, awaitReply, nil)
	if err != nil {
		return fmt.Errorf("bind dead queue to dead letter exchange: %w", err)
	}

	c.deadQueueName = dlQueue.Name

	queue, err := c.ch.QueueDeclare(c.queueName,
		isDurable, noAutodelete, notExclusive, awaitReply, amqp.Table{
			"x-queue-type": "classic",
			"x-overflow":   "reject-publish",
			// Queue messages for retry if they are discarded.
			"x-dead-letter-exchange":    deadletterEx,
			"x-dead-letter-routing-key": routeScheduleRetry,
		})
	if err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	err = c.ch.QueueBind(
		queue.Name, routeDoRetry, deadletterEx, awaitReply, nil)
	if err != nil {
		return fmt.Errorf("bind work queue to retries from dead letter exchange: %w", err)
	}

	if len(c.routingKeys) == 0 {
		err = c.ch.QueueBind(
			queue.Name, routeAll, c.exchangeName, awaitReply, nil)
		if err != nil {
			return fmt.Errorf("bind to exchange: %w", err)
		}
	}

	for _, key := range c.routingKeys {
		err = c.ch.QueueBind(
			queue.Name, key, c.exchangeName, awaitReply, nil)
		if err != nil {
			return fmt.Errorf("bind to routing key %q: %w",
				key, err)
		}
	}

	return nil
}

func (c *Consumer) connect() error {
	config := amqp.Config{Properties: amqp.NewConnectionProperties()}

	if c.connectionName != "" {
		config.Properties.SetClientConnectionName(c.connectionName)
	}

	conn, err := amqp.DialConfig(c.uri, config)
	if err != nil {
		return fmt.Errorf("AMQP dial: %w", err)
	}

	c.conn = conn
	c.connClosed = conn.NotifyClose(make(chan *amqp.Error, 1))

	return nil
}

func (c *Consumer) openChan() error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}

	err = ch.Qos(5, 0, false)
	if err != nil {
		return fmt.Errorf("set pre-fetch QoS: %w", err)
	}

	c.ch = ch
	c.chanCancel = ch.NotifyCancel(make(chan string, 1))
	c.chanClose = ch.NotifyClose(make(chan *amqp.Error, 1))

	return nil
}

func firstNonZeroDuration(values ...time.Duration) time.Duration {
	for _, d := range values {
		if d != 0 {
			return d
		}
	}

	return 0
}

// GetRoutingKey searches for the original routing key in headers or
// returns the passed routingKey if it's recognized as original.
func GetRoutingKey(routingKey string, headers amqp.Table, queueName string) (string, error) {
	// routingKey is equal to queueName when msgs are manually shoveled from deadletter queue
	// via rabbit ui shoveler interface.
	if routingKey != "do-retry" && routingKey != queueName && routingKey != "" {
		return routingKey, nil
	}

	searchInDeathField := func(field string) (string, error) {
		deaths, ok := headers[field].([]interface{})
		if !ok || len(deaths) == 0 {
			return "", fmt.Errorf("missing %q field or has unexpected type or is empty", field)
		}

		for i := range deaths {
			death, ok := deaths[i].(amqp.Table)
			if !ok {
				continue
			}

			qn, ok := death["queue"].(string)
			if !ok || qn != queueName {
				continue
			}

			rKeys, ok := death["routing-keys"].([]interface{})
			if !ok || len(rKeys) == 0 {
				return "", fmt.Errorf("missing routing key for queue %q in %q field", queueName, field)
			}

			rk, ok := rKeys[0].(string)
			if !ok {
				return "", fmt.Errorf(
					"expected routing key of type string at position 0 for queue %q in %q field", queueName, field)
			}

			return rk, nil
		}

		return "", fmt.Errorf("no matching queue found for %q queue in %q field", queueName, field)
	}

	routingKey, xErr := searchInDeathField("x-death")
	if xErr == nil {
		return routingKey, nil
	}

	routingKey, oErr := searchInDeathField("old-death")
	if oErr == nil {
		return routingKey, nil
	}

	return "", fmt.Errorf("routing key not found: %w", errors.Join(xErr, oErr))
}
