package match

import "testing"

func TestMatchPattern(t *testing.T) {
	type args struct {
		pattern string
		key     string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "exact match",
			args: args{pattern: "foo", key: "foo"},
			want: true,
		},
		{
			name: "exact match with wildcard",
			args: args{pattern: "foo*", key: "foo123"},
			want: true,
		},
		{
			name: "no match",
			args: args{pattern: "foo", key: "bar"},
			want: false,
		},
		{
			name: "partial match",
			args: args{pattern: "foo", key: "foobar"},
			want: false,
		},
		{
			name: "partial match with wildcard",
			args: args{pattern: "foo*", key: "foobar123"},
			want: true,
		},
		{
			name: "empty pattern",
			args: args{pattern: "", key: "foo"},
			want: false,
		},
		{
			name: "empty key",
			args: args{pattern: "foo", key: ""},
			want: false,
		},
		{
			name: "wildcard only",
			args: args{pattern: "*", key: "foo"},
			want: true,
		},
		{
			name: "wildcard only with multiple segments",
			args: args{pattern: "*", key: "foo:bar:baz"},
			want: true,
		},
		{
			name: "wildcard only with empty key",
			args: args{pattern: "*", key: ""},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MatchPattern(tt.args.pattern, tt.args.key); got != tt.want {
				t.Errorf("MatchPattern() = %v, want %v", got, tt.want)
			}
		})
	}
}
