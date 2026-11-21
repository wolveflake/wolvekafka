// Copyright 2023-2026 Wolveflake LLC
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// GroupStatus reports whether a consumer is a member of its consumer group.
type GroupStatus struct {
	// Joined is true once the broker accepted the consumer into its group.
	Joined bool
	// LastError is the most recent Kafka error seen while not joined, such
	// as an unreachable broker or failed authentication.
	LastError string
}

// GroupStatus is safe to call from any goroutine.
func (c *Consumer) GroupStatus() GroupStatus {
	if c.franzClient == nil {
		// Test stubs have no group protocol.
		return GroupStatus{Joined: true}
	}
	member, generation := c.franzClient.GroupMetadata()
	if member != "" && generation >= 0 {
		return GroupStatus{Joined: true}
	}
	status := GroupStatus{}
	if err, ok := c.lastFetchErr.Load().(string); ok {
		status.LastError = err
	}
	return status
}

// groupErrorHook records errors that end a group session or join attempt,
// and failed broker connections, which franz-go otherwise retries silently.
type groupErrorHook struct{ c *Consumer }

func (h groupErrorHook) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err != nil {
		h.c.lastFetchErr.Store(fmt.Sprintf("connect to %s: %v", net.JoinHostPort(meta.Host, strconv.Itoa(int(meta.Port))), err))
	}
}

func (h groupErrorHook) OnGroupManageError(err error) {
	if err != nil {
		h.c.lastFetchErr.Store(err.Error())
	}
}
