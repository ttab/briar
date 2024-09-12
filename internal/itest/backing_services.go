package itest

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"golang.org/x/sync/errgroup"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	m    sync.Mutex
	bs   *BackingServices
	bErr error
)

type Environment struct {
	RabbitURI string
}

type T interface {
	Name() string
	Helper()
	Fatalf(format string, args ...any)
	Cleanup(func())
	Log(args ...any)
}

func SetUpBackingServices(
	t T,
) Environment {
	t.Helper()

	bs, err := getBackingServices()
	Must(t, err, "get backing services")

	t.Cleanup(func() {
		bs.refCount--

		if bs.refCount > 0 {
			return
		}

		err := PurgeBackingServices()
		Must(t, err, "purge backing services")

		t.Log("removed test containers")
	})

	env := Environment{
		RabbitURI: bs.getRabbitURI(),
	}

	return env
}

func PurgeBackingServices() error {
	m.Lock()
	defer m.Unlock()

	if bs == nil {
		return nil
	}

	return bs.Purge()
}

func getBackingServices() (bsOut *BackingServices, _ error) {
	m.Lock()
	defer m.Unlock()

	// Make sure that we bump ref count if we return the backing service.
	defer func() {
		if bsOut != nil {
			bsOut.refCount++
		}
	}()

	if bs != nil || bErr != nil {
		return bs, bErr
	}

	bs, bErr = createBackingServices()

	return bs, bErr
}

func createBackingServices() (*BackingServices, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, fmt.Errorf("failed to create docker pool: %w", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		return nil, fmt.Errorf("could not connect to docker: %w", err)
	}

	b := BackingServices{
		pool: pool,
	}

	var grp errgroup.Group

	grp.Go(b.bootstrapRabbit)

	err = grp.Wait()
	if err != nil {
		pErr := b.Purge()
		if pErr != nil {
			return nil, errors.Join(err, pErr)
		}

		return nil, err //nolint:wrapcheck
	}

	return &b, nil
}

type BackingServices struct {
	refCount int
	pool     *dockertest.Pool
	rabbit   *dockertest.Resource
}

func (bs *BackingServices) bootstrapRabbit() error {
	res, err := bs.pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "rabbitmq",
		Tag:        "3.13-alpine",
	}, func(hc *docker.HostConfig) {
		hc.AutoRemove = true
	})
	if err != nil {
		return fmt.Errorf("failed to run minio container: %w", err)
	}

	bs.rabbit = res

	// Make sure that containers don't stick around for more than an hour,
	// even if in-process cleanup fails.
	_ = res.Expire(3600)

	err = bs.pool.Retry(func() error {
		conn, err := amqp.Dial(bs.getRabbitURI())
		if err != nil {
			return fmt.Errorf("AMQP dial: %w", err)
		}
		defer conn.Close()

		ch, err := conn.Channel()
		if err != nil {
			return fmt.Errorf("open channel: %w", err)
		}
		defer ch.Close()

		err = ch.ExchangeDeclare("bs-healthcheck", amqp.ExchangeTopic,
			true, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("reate exchange: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to connect to rabbit: %w", err)
	}

	return nil
}

func (bs *BackingServices) getRabbitURI() string {
	return fmt.Sprintf(
		"amqp://guest:guest@localhost:%s",
		bs.rabbit.GetPort("5672/tcp"))
}

func (bs *BackingServices) Purge() error {
	var errs []error

	if bs.rabbit != nil {
		err := bs.pool.Purge(bs.rabbit)
		if err != nil {
			errs = append(errs, fmt.Errorf(
				"failed to purge rabbitmq container: %w", err))
		}
	}

	switch len(errs) {
	case 1:
		return errs[0]
	case 0:
		return nil
	default:
		return errors.Join(errs...)
	}
}
