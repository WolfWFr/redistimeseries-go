package redis_timeseries_go

// GetOptions represent the options for getting the last sample in a time-series
type GetOptions struct {
	Latest bool
}

func NewGetOptions() *GetOptions {
	return &GetOptions{
		Latest: false,
	}
}

// DefaultGetOptions are the default options for getting the last sample in a time-series
var DefaultGetOptions = *NewGetOptions()

// SetLatest determines whether the compacted value of the LATEST, possibly partial, bucket is reported.
// This option is ignored when the timeseries is not a compaction.
func (getOptions *GetOptions) SetLatest(latest bool) *GetOptions {
	getOptions.Latest = latest
	return getOptions
}

func createGetCmdArguments(key string, getOptions GetOptions) []interface{} {
	args := []interface{}{key}
	if getOptions.Latest {
		args = append(args, "LATEST")
	}
	return args
}
