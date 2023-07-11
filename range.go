package redis_timeseries_go

import (
	"fmt"
	"strconv"
)

// RangeOptions represent the options for querying across a time-series
type RangeOptions struct {
	Latest           bool
	AggType          AggregationType
	TimeBucket       int
	Count            int64
	Align            int64
	FilterByTs       []int64
	FilterByValueMin *float64
	FilterByValueMax *float64
	BucketTimestamp  BucketTimestamp
	Empty            bool
}

func NewRangeOptions() *RangeOptions {
	return &RangeOptions{
		Latest:           false,
		AggType:          "",
		TimeBucket:       -1,
		Count:            -1,
		Align:            -1,
		FilterByTs:       []int64{},
		FilterByValueMin: nil,
		FilterByValueMax: nil,
		BucketTimestamp:  "",
		Empty:            false,
	}
}

// DefaultRangeOptions are the default options for querying across a time-series range
var DefaultRangeOptions = *NewRangeOptions()

// SetLatest determines whether the compacted value of the LATEST, possibly partial, bucket is reported.
// This option is ignored when the timeseries is not a compaction.
func (rangeopts *RangeOptions) SetLatest(latest bool) *RangeOptions {
	rangeopts.Latest = latest
	return rangeopts
}

// SetEmpty sets the empty control for AGGREGATION.
// This determines whether aggregations for empty buckets are reported.
func (rangeopts *RangeOptions) SetEmpty(empty bool) *RangeOptions {
	rangeopts.Empty = empty
	return rangeopts
}

func (rangeopts *RangeOptions) SetCount(count int64) *RangeOptions {
	rangeopts.Count = count
	return rangeopts
}

// SetAlign sets the time bucket alignment control for AGGREGATION.
// This will control the time bucket timestamps by changing the reference timestamp on which a bucket is defined.
func (rangeopts *RangeOptions) SetAlign(byTimeStamp int64) *RangeOptions {
	rangeopts.Align = byTimeStamp
	return rangeopts
}

// SetBucketTimestamp sets the time bucket timestamp control for AGGREGATION.
// This will control how time bucket timestamps are reported.
func (rangeopts *RangeOptions) SetBucketTimestamp(bucketTimestamp BucketTimestamp) *RangeOptions {
	rangeopts.BucketTimestamp = bucketTimestamp
	return rangeopts
}

// SetFilterByTs sets a list of timestamps to filter the result by specific timestamps
func (rangeopts *RangeOptions) SetFilterByTs(filterByTS []int64) *RangeOptions {
	rangeopts.FilterByTs = filterByTS
	return rangeopts
}

// SetFilterByValue filters result by value using minimum and maximum ( inclusive )
func (rangeopts *RangeOptions) SetFilterByValue(min, max float64) *RangeOptions {
	rangeopts.FilterByValueMin = &min
	rangeopts.FilterByValueMax = &max
	return rangeopts
}

func (rangeopts *RangeOptions) SetAggregation(aggType AggregationType, timeBucket int) *RangeOptions {
	rangeopts.AggType = aggType
	rangeopts.TimeBucket = timeBucket
	return rangeopts
}

func createRangeCmdArguments(key string, fromTimestamp int64, toTimestamp int64, rangeOptions RangeOptions) []interface{} {
	args := []interface{}{key, strconv.FormatInt(fromTimestamp, 10), strconv.FormatInt(toTimestamp, 10)}
	if rangeOptions.Latest {
		args = append(args, "LATEST")
	}
	if rangeOptions.FilterByValueMin != nil {
		args = append(args, "FILTER_BY_VALUE",
			fmt.Sprintf("%f", *rangeOptions.FilterByValueMin),
			fmt.Sprintf("%f", *rangeOptions.FilterByValueMax))
	}
	if len(rangeOptions.FilterByTs) > 0 {
		args = append(args, "FILTER_BY_TS")
		for _, timestamp := range rangeOptions.FilterByTs {
			args = append(args, strconv.FormatInt(timestamp, 10))
		}
	}
	if rangeOptions.AggType != "" {
		args = append(args, "AGGREGATION", rangeOptions.AggType, strconv.Itoa(rangeOptions.TimeBucket))
	}
	if rangeOptions.Count != -1 {
		args = append(args, "COUNT", strconv.FormatInt(rangeOptions.Count, 10))
	}
	if rangeOptions.Align != -1 {
		args = append(args, "ALIGN", strconv.FormatInt(rangeOptions.Align, 10))
	}
	if rangeOptions.BucketTimestamp != "" {
		args = append(args, "BUCKETTIMESTAMP", rangeOptions.BucketTimestamp)
	}
	if rangeOptions.Empty {
		args = append(args, "EMPTY")
	}
	return args
}
