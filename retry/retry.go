package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Doer provides the function signature for the function that is retried if needed
type Doer = func() (interface{}, error)

// WantRetry provides the function signature for a custom function to determine if the doer function is retried,
// based on the returned error
type WantRetry = func(err error) bool

// ErrAttemptsExceededLimit is returned when the number of attempts has reached
// the maximum permitted
type ErrAttemptsExceededLimit struct {
	WrappedErr error
}

// Error provides the formatted error string for the ErrAttemptsExceededLimit type
func (e ErrAttemptsExceededLimit) Error() string {
	return fmt.Sprintf("number of attempts exceeded: %s", e.WrappedErr.Error())
}

// Do executes the doer function, and retries if needed up to maxAttempts
//   ctx - the context that if cancelled with prevent any more retry attempts
//   doer - the function that is executed and retried if needed
//   wantRetry - the function that determines if the doer is retried based on the returned error
//   maxAttempts - the maximum number of times the doer function will be attempted
//   retryTime - the initial sleep time between requests. The retryTime will exponentially increase on subsequent retries
func Do(
	ctx context.Context,
	doer Doer,
	wantRetry WantRetry,
	maxAttempts int,
	retryTime time.Duration,
) (res interface{}, err error) {

	for attempt := 1; attempt <= maxAttempts; attempt++ {

		// prioritise any context cancellation
		if ctx.Err() != nil {
			err = ctx.Err()
			return
		}

		if attempt > 1 {
			err = sleep(ctx, attempt, retryTime)
			if err != nil {
				return
			}
		}

		res, err = doer()
		if err == nil {
			return // success
		}

		if !wantRetry(err) {
			return
		}
	}

	return nil, ErrAttemptsExceededLimit{err}
}

// sleep waits for a calculated time. The time increases based on the attempt number. The function returns
// immediately if the given context is cancelled.
func sleep(ctx context.Context, attempt int, retryTime time.Duration) error {
	pingChan := make(chan struct{})

	go func() {
		time.Sleep(getSleepTime(attempt, retryTime))
		close(pingChan)
	}()

	// check for first of: context cancellation or sleep ends
	select {
	case <-pingChan:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// getSleepTime calculates a time which increases, based on the attempt and initial retry time.
// It uses the algorithm 2^n where n is the attempt number (double the previous) and
// a randomization factor of between 0-5ms so that the server isn't being hit constantly
// at the same time by many clients
func getSleepTime(attempt int, retryTime time.Duration) time.Duration {
	n := math.Pow(2, float64(attempt))
	rnd := time.Duration(rand.Intn(4)+1) * time.Millisecond
	return (time.Duration(n) * retryTime) - rnd
}
