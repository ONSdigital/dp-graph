package retry_test

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-graph/v3/retry"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_Do(t *testing.T) {

	ctx := context.Background()

	Convey("Given a Doer function that succeeds", t, func() {

		expectedRes := 123
		doerCallCount := 0
		maxAttempts := 1
		retryTime := time.Millisecond

		doer := func() (interface{}, error) {
			doerCallCount++
			return expectedRes, nil
		}
		wantRetry := func(err error) bool {
			return false
		}

		Convey("When do is called", func() {

			res, err := retry.Do(ctx, doer, wantRetry, maxAttempts, retryTime)

			Convey("Then the response from the doer function is returned", func() {
				So(doerCallCount, ShouldEqual, 1)
				So(res, ShouldEqual, expectedRes)
				So(err, ShouldBeNil)
			})
		})
	})

	Convey("Given a doer function that fails", t, func() {

		expectedErr := errors.New("error test message")
		doerCallCount := 0
		maxAttempts := 3
		retryTime := time.Millisecond * 0

		doer := func() (interface{}, error) {
			doerCallCount++
			return nil, expectedErr
		}
		wantRetry := func(err error) bool {
			return true
		}

		Convey("When do is called", func() {

			res, err := retry.Do(ctx, doer, wantRetry, maxAttempts, retryTime)

			Convey("Then the doer is retried the expected number of times", func() {
				So(doerCallCount, ShouldEqual, 3)
				So(err.Error(), ShouldEqual, "number of attempts exceeded: "+expectedErr.Error())
				So(res, ShouldBeNil)
			})
		})
	})

	Convey("Given a cancelled context", t, func() {

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		expectedErr := errors.New("error test message")
		doerCallCount := 0
		maxAttempts := 3
		retryTime := time.Millisecond * 0

		doer := func() (interface{}, error) {
			doerCallCount++
			return nil, expectedErr
		}
		wantRetry := func(err error) bool {
			return true
		}

		Convey("When do is called", func() {

			res, err := retry.Do(ctx, doer, wantRetry, maxAttempts, retryTime)

			Convey("Then the doer is not retried due to the cancelled context", func() {
				So(doerCallCount, ShouldEqual, 0)
				So(err.Error(), ShouldEqual, "context canceled")
				So(res, ShouldBeNil)
			})
		})
	})
}
