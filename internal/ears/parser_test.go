package ears

import "testing"

func TestClassify_Positive(t *testing.T) {
	cases := []struct {
		text string
		want Kind
	}{
		{"WHEN a POST /tasks arrives the system SHALL create task", Event},
		{"WHILE reload is in progress the system SHALL block", State},
		{"WHERE parent is DONE the system SHALL reopen it", Optional},
		{"IF business_value missing THEN the system SHALL respond 422", Unwanted},
		{"The system SHALL cache TASKOWNERS in memory", Ubiquitous},
		{"the system SHALL bump s.cache after commit", Ubiquitous},
	}
	for _, c := range cases {
		t.Run(c.text[:20], func(t *testing.T) {
			if got := Classify(c.text); got != c.want {
				t.Errorf("Classify(%q) = %s, want %s", c.text, got, c.want)
			}
		})
	}
}

func TestClassify_NegativeNonEARS(t *testing.T) {
	cases := []string{
		"unblocks the AWS cutover",
		"users can now trigger SMM on demand",
		"", // empty → non-ears
		"the system should probably handle this", // "should" ≠ SHALL
	}
	for _, text := range cases {
		if got := Classify(text); got != NonEARS {
			t.Errorf("Classify(%q) = %s, want NonEARS", text, got)
		}
	}
}

func TestClassify_IfThenBeatsUbiquitous(t *testing.T) {
	// A string with SHALL inside an IF...THEN should stay `unwanted`, not
	// slip through to `ubiquitous`. Priority order test.
	text := "IF error occurs THEN the system SHALL log"
	if got := Classify(text); got != Unwanted {
		t.Errorf("Classify(%q) = %s, want Unwanted (priority ordering broken)", text, got)
	}
}

func TestMatchesEARS_PositivePaths(t *testing.T) {
	cases := []string{
		"WHEN X the system SHALL Y",
		"WHILE X the system SHALL Y",
		"WHERE X the system SHALL Y",
		"IF fault THEN the system SHALL Y",
		"The system SHALL Y",
	}
	for _, text := range cases {
		if !MatchesEARS(text) {
			t.Errorf("MatchesEARS(%q) = false, want true", text)
		}
	}
}

func TestMatchesEARS_NegativePaths(t *testing.T) {
	cases := []string{
		"",                                    // empty
		"just a plain sentence",               // non-ears
		"unblocks the AWS cutover",            // non-ears
		"WHEN X the system should Y",          // recognised kind but "should" ≠ SHALL
		"WHILE X the system will Y",           // WHILE without SHALL
		"IF X THEN the system must Y",         // IF/THEN without SHALL
	}
	for _, text := range cases {
		if MatchesEARS(text) {
			t.Errorf("MatchesEARS(%q) = true, want false", text)
		}
	}
}
