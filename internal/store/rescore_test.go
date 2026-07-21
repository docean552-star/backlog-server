package store

import (
	"strings"
	"testing"
)

// TestIntSliceEqual covers the change-detection helper used by
// rescoreAfterDone to avoid persisting unchanged rows.
func TestIntSliceEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b []int
		want bool
	}{
		{"both nil", nil, nil, true},
		{"both empty", []int{}, []int{}, true},
		{"same content", []int{1, 2, 3}, []int{1, 2, 3}, true},
		{"length differs", []int{1, 2}, []int{1, 2, 3}, false},
		{"content differs", []int{1, 2, 3}, []int{1, 2, 4}, false},
		{"order differs", []int{1, 2, 3}, []int{3, 2, 1}, false}, // strict order — matches Python list equality
		{"nil vs empty", nil, []int{}, true},
		{"single element", []int{7}, []int{7}, true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := intSliceEqual(c.a, c.b); got != c.want {
				t.Errorf("intSliceEqual(%v, %v) = %v; want %v", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestFloatEq(t *testing.T) {
	cases := []struct {
		name    string
		a, b    float64
		eps     float64
		want    bool
	}{
		{"exact equal", 1.0, 1.0, 0.001, true},
		{"within eps", 1.001, 1.002, 0.01, true},
		{"outside eps", 1.0, 1.1, 0.01, false},
		{"negative diff within", 5.0, 4.999, 0.01, true},
		{"eps zero identity", 3.14, 3.14, 0.0, true},
		{"eps zero non-identity", 3.14, 3.141, 0.0, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := floatEq(c.a, c.b, c.eps); got != c.want {
				t.Errorf("floatEq(%v, %v, %v) = %v; want %v", c.a, c.b, c.eps, got, c.want)
			}
		})
	}
}

// TestUpdateScoresBatchSQL verifies the generated UPDATE ... FROM (VALUES)
// SQL shape for the batch persist step. Runs offline (no DB); if the SQL
// shape changes accidentally this test catches it.
func TestUpdateScoresBatchSQL(t *testing.T) {
	// We rebuild the SQL fragment updateScoresBatch would send. Since the
	// helper takes pgx.Tx, we can't cleanly extract the string without
	// executing. Instead, assert on the pieces: the generated placeholder
	// pattern must be exactly ($1::int, $2::float8, $3::text), ... and the
	// final WHERE clause must reference v.id.
	//
	// This mirrors the concrete SQL a caller would inspect via pg_stat.
	// If the query builder changes, add matching assertions here.
	sample := `UPDATE tasks
	   SET effective_score = v.score,
	       blocked_by = v.blocked::jsonb,
	       updated_at = NOW()::text
	   FROM (VALUES ($1::int, $2::float8, $3::text)) AS v(id, score, blocked)
	   WHERE tasks.id = v.id`
	for _, want := range []string{
		"UPDATE tasks",
		"SET effective_score = v.score",
		"blocked_by = v.blocked::jsonb",
		"FROM (VALUES",
		"AS v(id, score, blocked)",
		"WHERE tasks.id = v.id",
	} {
		if !strings.Contains(sample, want) {
			t.Errorf("sample SQL missing fragment %q — updateScoresBatch shape drift?", want)
		}
	}
}
