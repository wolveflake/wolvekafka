// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package wks

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxRetryAfter caps how long a single Retry-After can pause a batch, so a
// misconfigured handler cannot stall a partition for hours.
var maxRetryAfter = 5 * time.Minute

// throttledError reports that the handler asked WKS to slow down rather than
// failing the records: HTTP 429, or 503 with a Retry-After header.
type throttledError struct {
	status     int
	retryAfter time.Duration // zero when the handler did not say how long to wait
	err        error
}

func (e *throttledError) Error() string { return e.err.Error() }
func (e *throttledError) Unwrap() error { return e.err }

// throttleFromResponse classifies a non-2xx handler response. It returns nil
// when the response is an ordinary failure.
func throttleFromResponse(status int, header http.Header, now time.Time, err error) *throttledError {
	retryAfter, hasRetryAfter := parseRetryAfter(header.Get("Retry-After"), now)
	switch {
	case status == http.StatusTooManyRequests:
	case status == http.StatusServiceUnavailable && hasRetryAfter:
	default:
		return nil
	}
	return &throttledError{status: status, retryAfter: retryAfter, err: err}
}

// parseRetryAfter parses an HTTP Retry-After value given as delay-seconds or
// an HTTP date. It reports false for a missing or malformed value. Dates in the
// past yield zero; long delays are capped at maxRetryAfter.
func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	var delay time.Duration
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if seconds > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter, true
		}
		delay = time.Duration(seconds) * time.Second
	} else if at, err := http.ParseTime(value); err == nil {
		delay = at.Sub(now)
		if delay < 0 {
			delay = 0
		}
	} else {
		return 0, false
	}
	if delay > maxRetryAfter {
		delay = maxRetryAfter
	}
	return delay, true
}
