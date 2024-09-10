package briar

import (
	"errors"
	"fmt"
)

// TransitionCode are used to describe what step of the consumer process failed.
type TransitionCode string

const (
	CodeConnect      TransitionCode = "connect"
	CodeOpenChan     TransitionCode = "channel"
	CodeDeclare      TransitionCode = "declare"
	CodeConsume      TransitionCode = "consume"
	CodeDisconnected TransitionCode = "disconnected"
	CodeChanClosed   TransitionCode = "chan_closed"
	CodeHandlerFail  TransitionCode = "handler_fail"
	CodeCancelled    TransitionCode = "cancelled"
)

// HaltOnNthFailure is a convenience function for stopping retries after a
// number of consecutive failures.
func HaltOnNthFailure(code TransitionCode, n int) HaltFunc {
	return func(c TransitionCode, count int, _ error) bool {
		if c == code && count >= n {
			return true
		}

		return false
	}
}

func HaltErrorf(format string, a ...any) error {
	err := fmt.Errorf(format, a...)

	return &HaltError{
		cause: errors.Unwrap(err),
		msg:   err.Error(),
	}
}

type HaltError struct {
	msg   string
	cause error
}

func (he *HaltError) Error() string {
	return he.msg
}

func (he *HaltError) Unwrap() error {
	return he.cause
}

type transitionError struct {
	code  TransitionCode
	cause error
	msg   string
}

func (te *transitionError) Error() string {
	return te.msg
}

func (te *transitionError) Unwrap() error {
	return te.cause
}

func tErrorw(code TransitionCode, err error) *transitionError {
	return &transitionError{
		code:  code,
		cause: err,
		msg:   err.Error(),
	}
}

func tErrorf(code TransitionCode, format string, a ...any) *transitionError {
	err := fmt.Errorf(format, a...)

	return &transitionError{
		code:  code,
		cause: errors.Unwrap(err),
		msg:   err.Error(),
	}
}

func NewDeadletterError() error {
	return &DeadletterError{}
}

type DeadletterError struct{}

func (he *DeadletterError) Error() string {
	return "deadletter message"
}
