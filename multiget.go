package redis_timeseries_go

// MultiGetOptions represent the options for querying across multiple time-series
type MultiGetOptions struct {
	Latest     bool
	WithLabels bool
}

// MultiGetOptions are the default options for querying across multiple time-series
var DefaultMultiGetOptions = *NewMultiGetOptions()

func NewMultiGetOptions() *MultiGetOptions {
	return &MultiGetOptions{
		Latest:     false,
		WithLabels: false,
	}
}

// SetLatest determines whether the compacted value of the LATEST, possibly partial, bucket is reported.
// This option is ignored when the timeseries is not a compaction.
func (mgetOpts *MultiGetOptions) SetLatest(latest bool) *MultiGetOptions {
	mgetOpts.Latest = latest
	return mgetOpts
}

func (mgetopts *MultiGetOptions) SetWithLabels(value bool) *MultiGetOptions {
	mgetopts.WithLabels = value
	return mgetopts
}

func createMultiGetCmdArguments(mgetOptions MultiGetOptions, filters []string) []interface{} {
	args := []interface{}{}
	if mgetOptions.Latest {
		args = append(args, "LATEST")
	}
	if mgetOptions.WithLabels {
		args = append(args, "WITHLABELS")
	}
	args = append(args, "FILTER")
	for _, filter := range filters {
		args = append(args, filter)
	}
	return args
}
