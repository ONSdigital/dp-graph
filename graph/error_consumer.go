package graph

import (
	"context"
	"errors"
	"github.com/ONSdigital/log.go/log"
)

var ErrContextDone = errors.New("context done while closing error consumer")

// ErrorConsumer maintains a go routine to consume an error channel
type ErrorConsumer struct {
	closing chan interface{}
	closed  chan interface{}
}

func NewLoggingErrorConsumer(ctx context.Context, errors chan error) *ErrorConsumer {
	return NewErrorConsumer(errors, func(err error) {
		log.Event(ctx, "error from graph DB", log.ERROR, log.Error(err))
	})
}

// NewErrorConsumer starts a new go routine to consume errors
func NewErrorConsumer(errors chan error, consume func(error)) *ErrorConsumer {

	c := &ErrorConsumer{
		closing: make(chan interface{}),
		closed:  make(chan interface{}),
	}

	go func() {
		defer close(c.closed)

		for {
			select {
			case err := <-errors:
				consume(err)
			case <-c.closing:
				return
			}
		}
	}()

	return c
}

// Close blocks until the go routine has finished
func (c *ErrorConsumer) Close(ctx context.Context) error {
	close(c.closing)

	select {
	case <-c.closed:
		return nil
	case <-ctx.Done():
		return ErrContextDone
	}
}
