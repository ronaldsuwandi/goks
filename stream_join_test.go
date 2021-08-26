package goks

import (
	"context"
	"testing"
	"time"
)

type processOutput struct {
	kvc        KeyValueContext
	downstream bool
}

func newKvc(key string, value string, timestamp time.Time) KeyValueContext {
	return KeyValueContext{
		Key: key,
		ValueContext: ValueContext{
			Value: value,
			Ctx:   context.WithValue(context.Background(), Timestamp, timestamp),
		},
	}
}

func TestStreamJoiner_InnerJoinTable(t *testing.T) {
	now := time.Now()

	type processInput struct {
		kvc KeyValueContext
		src Node
	}

	inputStream := &Stream{}
	table := &Table{}

	tests := []struct {
		name  string
		input []processInput
		want  []processOutput
	}{
		{
			name: "test",
			input: []processInput{
				{
					kvc: newKvc("key", "original", now),
					src: inputStream,
				},
				{
					kvc: newKvc("key", "from-table", now.Add(time.Second)),
					src: table,
				},
				{
					kvc: newKvc("key", "original2", now.Add(2*time.Second)),
					src: inputStream,
				},
			},
			want: []processOutput{
				{
					kvc:        newKvc("key", "original", now),
					downstream: false,
				},
				{
					kvc:        newKvc("key", "from-table", now.Add(time.Second)),
					downstream: false,
				},
				{
					kvc:        newKvc("key", "original2-joined-from-table", now.Add(2*time.Second)),
					downstream: true,
				},
			},
		},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			s := inputStream.InnerJoinTable(table, func(left KeyValueContext, right KeyValueContext) KeyValueContext {
				kvc := KeyValueContext{}
				kvc.Value = left.Value.(string) + "-joined-" + right.Value.(string)
				return kvc
			})

			var got []processOutput

			for _, i := range tt.input {
				resultKvc, downstream := s.processFn(i.kvc, i.src)
				got = append(got, processOutput{kvc: resultKvc, downstream: downstream})
			}

			if len(tt.want) != len(got) {
				t.Errorf("mismatch length. got = %d, want = %d", len(got), len(tt.want))
			}

			for i, g := range got {
				expected := tt.want[i]
				if g.kvc.Value != expected.kvc.Value {
					t.Errorf("mismatch value. got = %v, want = %v", g.kvc.Value, expected.kvc.Value)
				}
				if g.downstream != expected.downstream {
					t.Errorf("mismatch value. got = %v, want = %v", g.downstream, expected.downstream)
				}
			}

		})
	}
}

// TODO implement something like topology test driver
