package redis_timeseries_go

import (
	"reflect"
	"testing"
)

func TestCreateGetCmdArguments(t *testing.T) {
	type args struct {
		key        string
		getOptions GetOptions
	}
	tests := []struct {
		name string
		args args
		want []interface{}
	}{
		{"default", args{"key", DefaultGetOptions}, []interface{}{"key"}},
		{"latest", args{"key", *NewGetOptions().SetLatest(true)}, []interface{}{"key", "LATEST"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createGetCmdArguments(tt.args.key, tt.args.getOptions); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateGetCmdArguments() = %v, want %v", got, tt.want)
			}
		})
	}
}
