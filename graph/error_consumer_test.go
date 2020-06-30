package graph

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestChannelConsumer_Close(t *testing.T) {

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*200)
	errorChan := make(chan error, 1)
	consume := func(error) {}

	Convey("Given a channel consumer", t, func() {

		errorConsumer := NewErrorConsumer(errorChan, consume)

		Convey("When close is called", func() {

			err := errorConsumer.Close(ctx)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestChannelConsumer_CloseContext(t *testing.T) {

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*10)
	errorChan := make(chan error, 1)

	consumeFinished := false
	consume := func(error) {
		time.Sleep(time.Second)
		consumeFinished = true
	}

	Convey("Given a channel consumer on a long running function", t, func() {

		errorConsumer := NewErrorConsumer(errorChan, consume)
		errorChan <- errors.New("")

		Convey("When close is called", func() {

			err := errorConsumer.Close(ctx)

			Convey("Then a context timeout error is returned", func() {
				So(errors.Is(err, ErrContextDone), ShouldBeTrue)
			})

			Convey("Then the consume function did not finish", func() {
				So(consumeFinished, ShouldBeFalse)
			})
		})
	})
}

func TestNewChannelConsumer(t *testing.T) {

	consumeCalled := false
	consumeFinished := make(chan interface{})

	errorChan := make(chan error, 1)
	consume := func(error) {
		consumeCalled = true
		consumeFinished <- nil
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*200)

	Convey("Given a channel consumer", t, func() {

		errorConsumer := NewErrorConsumer(errorChan, consume)
		defer errorConsumer.Close(ctx)

		Convey("When an error is available on the configured channel", func() {

			errorChan <- errors.New("")

			select {
			case <-consumeFinished:
			case <-ctx.Done():
			}

			Convey("Then the consumer function is called", func() {
				So(consumeCalled, ShouldBeTrue)
			})
		})
	})
}
