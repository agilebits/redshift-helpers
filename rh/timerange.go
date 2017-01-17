package rh

import (
	"fmt"
	"time"
)

// TimeRange is used to limit records selected from the database
type TimeRange struct {
	From time.Time
	To   time.Time
}

// NewTimeRange returns one hour range with the given start time
func NewTimeRange(start time.Time) TimeRange {
	return TimeRange{
		From: start,
		To:   start.Add(time.Hour),
	}
}

func (tr *TimeRange) String() string {
	return fmt.Sprintf("TimeRange:{%v - %v}", tr.From, tr.To)
}

// NextHour returns a new time range for the next hour
func (tr *TimeRange) NextHour() TimeRange {
	result := *tr
	result.From = result.To
	result.To = result.From.Add(time.Hour)
	return result
}
