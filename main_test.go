package briar_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/ttab/briar/internal/itest"
)

func TestMain(m *testing.M) {
	exitVal := m.Run()

	err := itest.PurgeBackingServices()
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"failed to clean up backend services: %v\n", err)
	}

	os.Exit(exitVal)
}
