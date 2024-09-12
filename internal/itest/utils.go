package itest

import "context"

func Must(t T, err error, action string) {
	t.Helper()

	if err != nil {
		t.Fatalf("failed: %s: %v", action, err)
	}
}

func Context(t T) context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	t.Cleanup(cancel)

	return ctx
}
