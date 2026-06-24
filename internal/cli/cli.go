// Package cli implements the client-side subcommands (everything except `serve`).
//
// Each subcommand parses its own flags, builds a client.Client from config, fires
// one HTTP call against backlog-server, and prints either human-readable text or
// --json. Output of `next` mirrors `python3 backlogist.py next <agent>` so the
// switch is drop-in for any operator who eyeballs the listing.
package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/docean552-star/backlog-server/internal/client"
	"github.com/docean552-star/backlog-server/internal/config"
	"github.com/docean552-star/backlog-server/internal/store"
)

const clientTimeout = 8 * time.Second

// Run dispatches subcommand args to handlers. argv excludes the program name.
// Returns nil on success, error for the main() to print and exit non-zero.
func Run(argv []string) error {
	if len(argv) == 0 {
		return errMissingSubcommand
	}
	sub := argv[0]
	rest := argv[1:]
	switch sub {
	case "next":
		return runNext(rest)
	case "status":
		return runStatus(rest)
	case "info":
		return runInfo(rest)
	case "tasks":
		return runTasks(rest)
	case "healthz":
		return runHealthz(rest)
	}
	return fmt.Errorf("unknown subcommand: %s", sub)
}

var errMissingSubcommand = fmt.Errorf("missing subcommand")

func newClient() (*client.Client, error) {
	cfg := config.Load()
	if err := cfg.Validate(config.ModeClient); err != nil {
		return nil, err
	}
	return client.New(strings.TrimRight(cfg.ServerURL, "/"), cfg.AgentKey), nil
}

func parseFlags(name string, args []string, register func(fs *flag.FlagSet)) (*flag.FlagSet, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard) // we print our own errors
	register(fs)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return fs, nil
}

func runNext(args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return fmt.Errorf("usage: backlog-server next <agent> [--limit=N] [--json]")
	}
	agent := args[0]
	var (
		limit   int
		jsonOut bool
	)
	if _, err := parseFlags("next", args[1:], func(fs *flag.FlagSet) {
		fs.IntVar(&limit, "limit", 5, "max candidates")
		fs.BoolVar(&jsonOut, "json", false, "emit raw JSON")
	}); err != nil {
		return err
	}
	c, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	res, _, err := c.NextForAgent(ctx, agent, limit)
	if err != nil {
		return err
	}
	if jsonOut {
		return emitJSON(res)
	}
	formatNext(os.Stdout, agent, res)
	return nil
}

func formatNext(w io.Writer, agent string, r store.NextResult) {
	fmt.Fprintf(w, "=== Next tasks for Agent %s ===\n\n", strings.ToUpper(agent))
	for i, c := range r.Candidates {
		fmt.Fprintf(w, "#%d  #%d [score %s, readiness %s] %s\n",
			i+1, c.ID, fmtFloat(c.RankScore), fmtFloat(c.Readiness), c.Title)
		fmt.Fprintf(w, "    Reason: %s\n", c.Reason)
		if c.TaskPlan != "" {
			fmt.Fprintf(w, "    Task plan: %s\n", c.TaskPlan)
		}
		fmt.Fprintln(w)
	}
	if len(r.Blocked) > 0 {
		fmt.Fprintf(w, "--- Blocked (%d) ---\n", len(r.Blocked))
		for _, b := range r.Blocked {
			fmt.Fprintf(w, "  %s\n", b)
		}
		fmt.Fprintln(w)
	}
	if len(r.Conflicts) > 0 {
		fmt.Fprintln(w, "--- Conflict zones ---")
		for _, c := range r.Conflicts {
			fmt.Fprintf(w, "  %s\n", c)
		}
	}
}

func runStatus(args []string) error {
	var jsonOut bool
	_, err := parseFlags("status", args, func(fs *flag.FlagSet) {
		fs.BoolVar(&jsonOut, "json", false, "emit raw JSON")
	})
	if err != nil {
		return err
	}
	c, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	counts, _, err := c.StatusCounts(ctx)
	if err != nil {
		return err
	}
	if jsonOut {
		return emitJSON(counts)
	}
	statuses := make([]string, 0, len(counts))
	for s := range counts {
		statuses = append(statuses, s)
	}
	sort.Strings(statuses)
	total := 0
	for _, s := range statuses {
		fmt.Printf("  %-22s %d\n", s, counts[s])
		total += counts[s]
	}
	fmt.Printf("  %-22s %d\n", "TOTAL", total)
	return nil
}

func runInfo(args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return fmt.Errorf("usage: backlog-server info <id> [--json]")
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("id must be integer, got %q", args[0])
	}
	var jsonOut bool
	if _, err := parseFlags("info", args[1:], func(fs *flag.FlagSet) {
		fs.BoolVar(&jsonOut, "json", false, "emit raw JSON")
	}); err != nil {
		return err
	}
	c, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	t, _, err := c.GetTask(ctx, id)
	if err != nil {
		return err
	}
	if jsonOut {
		return emitJSON(t)
	}
	formatTask(os.Stdout, t)
	return nil
}

func formatTask(w io.Writer, t store.Task) {
	fmt.Fprintf(w, "#%d  %s\n", t.ID, t.Title)
	fmt.Fprintf(w, "  status:          %s\n", t.Status)
	fmt.Fprintf(w, "  owner:           %s\n", t.Owner)
	fmt.Fprintf(w, "  mode:            %s\n", t.Mode)
	fmt.Fprintf(w, "  workflow:        %s\n", t.Workflow)
	fmt.Fprintf(w, "  effective_score: %s\n", fmtFloat(t.EffectiveScore))
	if len(t.BlockedBy) > 0 {
		ids := make([]string, len(t.BlockedBy))
		for i, b := range t.BlockedBy {
			ids[i] = fmt.Sprintf("#%d", b)
		}
		fmt.Fprintf(w, "  blocked_by:      %s\n", strings.Join(ids, ", "))
	}
	if t.TaskPlan != "" {
		fmt.Fprintf(w, "  task_plan:       %s\n", t.TaskPlan)
	}
	if t.Spec != "" {
		fmt.Fprintf(w, "  spec:            %s\n", t.Spec)
	}
	if len(t.DoneWhen) > 0 {
		fmt.Fprintln(w, "  done_when:")
		for _, d := range t.DoneWhen {
			fmt.Fprintf(w, "    - %s\n", d)
		}
	}
	if t.Why != "" {
		fmt.Fprintf(w, "  why: %s\n", t.Why)
	}
	if t.BusinessValue != "" {
		fmt.Fprintf(w, "  business_value: %s\n", t.BusinessValue)
	}
}

func runTasks(args []string) error {
	var (
		owner   string
		status  string
		jsonOut bool
	)
	_, err := parseFlags("tasks", args, func(fs *flag.FlagSet) {
		fs.StringVar(&owner, "owner", "", "filter by owner")
		fs.StringVar(&status, "status", "", "filter by status")
		fs.BoolVar(&jsonOut, "json", false, "emit raw JSON")
	})
	if err != nil {
		return err
	}
	c, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	tasks, _, err := c.ListTasks(ctx, owner, status)
	if err != nil {
		return err
	}
	if jsonOut {
		return emitJSON(tasks)
	}
	for _, t := range tasks {
		fmt.Printf("#%-5d %-18s %-10s score=%-5s %s\n",
			t.ID, t.Status, t.Owner, fmtFloat(t.EffectiveScore), truncate(t.Title, 60))
	}
	fmt.Printf("(%d tasks)\n", len(tasks))
	return nil
}

func runHealthz(args []string) error {
	_, err := parseFlags("healthz", args, func(fs *flag.FlagSet) {})
	if err != nil {
		return err
	}
	c, err := newClient()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	_, ok, err := c.Healthz(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("server reports not ok")
	}
	fmt.Println("ok")
	return nil
}

func emitJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func fmtFloat(f float64) string {
	// Python prints '114.6' / '0.6' — single decimal, no trailing zero stripping
	// because Python's repr() of round(x, 1) keeps the .0. We strip only for ints.
	s := strconv.FormatFloat(f, 'f', 1, 64)
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
