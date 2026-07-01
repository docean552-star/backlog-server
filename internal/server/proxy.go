package server

// Subprocess proxy for backlogist write commands.
//
// Clients (Mac, dev machines) can't reach the private RDS endpoint and can't
// run the Python WRITE commands locally. This handler forks a backlogist
// subprocess on the server (which DOES have VPC access to RDS) and returns
// the stdout/stderr/exit_code as JSON. The server is the chokepoint —
// auth is enforced by the existing X-Agent-Key middleware.
//
// Why a generic /exec instead of per-command REST endpoints: a backlogist
// command's behaviour (gate checks, audit trail, parent cascade, recurrence,
// closure-reviewer integration) is rich and changes regularly. Re-implementing
// that surface in Go would double the maintenance cost. Forking the same
// Python that the operator uses on a laptop keeps a single source of truth.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"time"
)

const (
	execTimeout    = 60 * time.Second
	execMaxArgv    = 50
	execMaxArgLen  = 16 * 1024  // 16 KB per arg — pager messages can be multi-KB
	execMaxOutSize = 256 * 1024 // 256 KB per stream
	execWorkDir    = "/opt/apps/ax"
	execPython     = "/opt/apps/ax/.venv-server/bin/python"
	execMaxBodyB   = 1 * 1024 * 1024 // 1 MB total body (50 args × 16 KB + headers slack)
)

// Agent identifier is allowed to be only [A-Za-z0-9_-], 1..50 chars. This
// matches AX_AGENT values across the team (a, b, t1, t2, meta, samvel, angel,
// seo, smm, deva1, deva2, ...) and prevents env-injection via weird chars.
var execAgentRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,50}$`)

// ExecRequest is the JSON body for POST /exec.
// `stdin`, if present, is piped into the subprocess as-is — used by hook-* commands
// which receive a JSON event payload on stdin from the Claude Code hook protocol.
// `module` selects which Python module to run; defaults to "backlogist" for backward
// compat. Other allowed: "pager".
type ExecRequest struct {
	Agent  string   `json:"agent"`
	Argv   []string `json:"argv"`
	Stdin  string   `json:"stdin,omitempty"`
	Module string   `json:"module,omitempty"`
}

// Whitelist of Python modules the proxy is allowed to invoke. Lets us forward
// pager + backlogist (and future packages) through the same endpoint without
// turning /exec into an arbitrary-RCE for any X-Agent-Key holder.
var execAllowedModules = map[string]bool{
	"backlogist": true,
	"pager":      true,
}

// ExecResponse mirrors what a local CLI invocation would have produced.
type ExecResponse struct {
	ExitCode  int    `json:"exit_code"`
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
	Truncated bool   `json:"truncated,omitempty"`
	TimedOut  bool   `json:"timed_out,omitempty"`
}

func (s *Server) handleExec(w http.ResponseWriter, r *http.Request) {
	var req ExecRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, execMaxBodyB)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if !execAgentRe.MatchString(req.Agent) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent must match [A-Za-z0-9_-]{1,50}"})
		return
	}
	if n := len(req.Argv); n == 0 || n > execMaxArgv {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "argv must have 1..50 items"})
		return
	}
	for _, a := range req.Argv {
		if len(a) > execMaxArgLen {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "argv item exceeds 16 KB"})
			return
		}
	}

	module := req.Module
	if module == "" {
		module = "backlogist"
	}
	if !execAllowedModules[module] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "module not allowed: " + module})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), execTimeout)
	defer cancel()

	// Best-effort git pull before every backlogist subprocess. This is the fix
	// for the "frozen /opt/apps/ax" bug: without it, the tree only reflected
	// the tar-based bootstrap snapshot; a client's just-pushed docs/specs/*
	// were invisible to gate checks that Python reads off disk.
	//
	// Notes:
	//  - 5s timeout keeps a slow origin from starving the request.
	//  - --autostash lets us survive any transient local edits (someone
	//    running scripts on the box directly). --rebase over origin/main
	//    means the tree matches origin exactly on success.
	//  - Errors are logged, NOT returned: the subprocess still runs against
	//    whatever is currently on disk (which is still fresher than not
	//    pulling). Failure modes: network to GitHub down, or the tree drifted.
	//  - Skip for `pager` — that path never touches spec files.
	if module == "backlogist" {
		pullCtx, cancelPull := context.WithTimeout(ctx, 5*time.Second)
		pull := exec.CommandContext(pullCtx, "git", "-C", execWorkDir,
			"pull", "--rebase", "--autostash", "origin", "main")
		pull.Env = os.Environ()
		if out, err := pull.CombinedOutput(); err != nil {
			log.Printf("git pull /opt/apps/ax failed (continuing): %s: %v",
				bytes.TrimSpace(out), err)
		}
		cancelPull()
	}

	// Build argv: python -m <module> <user argv...>
	pyArgs := append([]string{"-m", module}, req.Argv...)
	cmd := exec.CommandContext(ctx, execPython, pyArgs...)
	cmd.Dir = execWorkDir
	// Inherit server env (PATH, PG creds via .env loader inside Python) and override AX_AGENT
	// so the right identity attaches to audit_trail rows. BACKLOGIST_AGENT also kept in sync.
	cmd.Env = append(
		append([]string{}, os.Environ()...),
		"AX_AGENT="+req.Agent,
		"BACKLOGIST_AGENT="+req.Agent,
		// Server-mode env removed so the subprocess goes direct to PG (no recursion loop).
		"BACKLOGIST_SERVER_URL=",
		"BACKLOGIST_AGENT_KEY=",
	)

	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	if req.Stdin != "" {
		cmd.Stdin = bytes.NewReader([]byte(req.Stdin))
	}

	runErr := cmd.Run()
	resp := ExecResponse{
		Stdout: outBuf.String(),
		Stderr: errBuf.String(),
	}
	if len(resp.Stdout) > execMaxOutSize {
		resp.Stdout = resp.Stdout[:execMaxOutSize] + "\n... [truncated by server]"
		resp.Truncated = true
	}
	if len(resp.Stderr) > execMaxOutSize {
		resp.Stderr = resp.Stderr[:execMaxOutSize] + "\n... [truncated by server]"
		resp.Truncated = true
	}

	if runErr != nil {
		if ctx.Err() == context.DeadlineExceeded {
			resp.TimedOut = true
			resp.ExitCode = 124 // standard `timeout` convention
		} else if ee, ok := runErr.(*exec.ExitError); ok {
			resp.ExitCode = ee.ExitCode()
		} else {
			// Failed to even start the subprocess (binary missing, fork failure)
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "exec failed: " + runErr.Error(),
			})
			return
		}
	}

	writeJSON(w, http.StatusOK, resp)
}
