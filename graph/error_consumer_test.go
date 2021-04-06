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

			Convey("Then no error is returned if close is called again", func() {
				err = errorConsumer.Close(ctx)
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestChannelConsumer_CloseContext(t *testing.T) {

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*10)
	errorChan := make(chan error, 1)

	chConsumingTest := make(chan struct{})
	consume := func(error) {
		// consume function closes the testing channel to notify that the consumer function has started
		close(chConsumingTest)
		// then sleep for an amount of time substantially greater than the timeout
		time.Sleep(time.Second)
	}

	Convey("Given a channel consumer on a long running function", t, func() {

		errorConsumer := NewErrorConsumer(errorChan, consume)
		errorChan <- errors.New("")

		Convey("When close is called", func() {

			// block until chConsumingTest is closed,
			// to be sure that the consume function has been triggered before trying to close it.
			<-chConsumingTest
			err := errorConsumer.Close(ctx)

			Convey("Then a context timeout error is returned", func() {
				So(err, ShouldBeError, ErrContextDone)
			})
		})
	})
}

func TestNewChannelConsumer(t *testing.T) {

	consumeFinished := make(chan interface{})

	errorChan := make(chan error, 1)
	consume := func(error) {
		consumeFinished <- nil
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*200)

	Convey("Given a channel consumer", t, func() {

		errorConsumer := NewErrorConsumer(errorChan, consume)
		defer errorConsumer.Close(ctx)

		Convey("When an error is available on the configured channel", func() {

			errorChan <- errors.New("")

			Convey("Then the consumer function completes", func() {
				select {
				case <-consumeFinished:
				case <-ctx.Done():
					t.Error("context time out waiting for error to be consumed")
				}
			})
		})
	})
}
