package scoring

// blocksReachability mirrors _blocks_reachability from
// backlogist/core/scoring.py:114-134.
//
// blocksMap[x] = tasks that x blocks. Result maps each node that appears as a
// blocked-task (i.e. a value in blocksMap) to the set of nodes reachable via
// forward traversal of blocksMap. Used by the blocker_value cycle-guard: an
// edge x→y is a back-edge iff y can reach x transitively; such edges are
// dropped from propagation so cycles cannot inflate potentials.
func blocksReachability(blocksMap map[int][]int) map[int]map[int]bool {
	reachable := func(start int) map[int]bool {
		seen := map[int]bool{}
		stack := append([]int(nil), blocksMap[start]...)
		for len(stack) > 0 {
			n := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if seen[n] {
				continue
			}
			seen[n] = true
			stack = append(stack, blocksMap[n]...)
		}
		return seen
	}

	out := map[int]map[int]bool{}
	// Only nodes that appear AS a blocked-task (value in map) need keys.
	for _, bts := range blocksMap {
		for _, bt := range bts {
			if _, ok := out[bt]; !ok {
				out[bt] = reachable(bt)
			}
		}
	}
	return out
}
