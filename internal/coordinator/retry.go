package coordinator

import (
	"context"
	"fmt"
	"math"
	"time"
)

// retryWithBackoff executes fn with exponential backoff up to maxRetries times.
//
// Shared by all three coordinators because they retried identically: the write,
// delete and delete-version flows each carried their own copy of this loop, and
// the only thing that ever differed between them was which config struct held
// the two numbers.
//
// Cancellation wins over the budget: ctx is checked before every attempt and
// while waiting out the backoff, so a caller that gives up is not made to wait
// out the rest of a schedule nobody will use.
func retryWithBackoff(ctx context.Context, maxRetries, retryBaseIntervalMS int, fn func() error) error {
	base := time.Duration(retryBaseIntervalMS) * time.Millisecond

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt < maxRetries {
			backoff := base * time.Duration(int64(math.Pow(2, float64(attempt))))
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return fmt.Errorf("retry exhausted after %d attempts: %w", maxRetries+1, lastErr)
}
