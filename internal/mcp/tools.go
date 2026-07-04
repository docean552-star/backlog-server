package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// toolClient wraps the HTTP calls against backlog-server. Each tool
// handler goes through this so the sidecar's translation logic is
// homogeneous (auth header, timeout, error unwrap).
type toolClient struct {
	baseURL string
	key     string
	http    *http.Client
}

func newToolClient(baseURL, agentKey string) *toolClient {
	return &toolClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		key:     agentKey,
		http:    &http.Client{Timeout: 60 * time.Second},
	}
}

func (c *toolClient) do(method, path string, body any) (int, []byte, error) {
	var buf io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal body: %w", err)
		}
		buf = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.baseURL+path, buf)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-Agent-Key", c.key)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	return resp.StatusCode, out, err
}

// callGET performs a GET and unwraps the response into a map for the tool
// result (MCP tools return JSON objects). Non-2xx yields an error whose
// message includes the server body — surfaced to Claude via isError=true.
func (c *toolClient) callGET(path string) (any, error) {
	code, body, err := c.do(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	return unwrapResponse(code, body)
}

func (c *toolClient) callPOST(path string, body any) (any, error) {
	code, resp, err := c.do(http.MethodPost, path, body)
	if err != nil {
		return nil, err
	}
	return unwrapResponse(code, resp)
}

func (c *toolClient) callPATCH(path string, body any) (any, error) {
	code, resp, err := c.do(http.MethodPatch, path, body)
	if err != nil {
		return nil, err
	}
	return unwrapResponse(code, resp)
}

func unwrapResponse(code int, body []byte) (any, error) {
	var payload any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			// Non-JSON body: preserve as string so the operator sees the raw
			// server output rather than a decode error.
			payload = string(body)
		}
	}
	if code >= 200 && code < 300 {
		return payload, nil
	}
	return payload, fmt.Errorf("backlog-server HTTP %d: %v", code, payload)
}

// ---------------------------------------------------------------------------
// Tool registry
// ---------------------------------------------------------------------------

type tool struct {
	name        string
	description string
	schema      map[string]any
	handler     func(json.RawMessage) (any, error)
}

type registry struct {
	tools []tool
	index map[string]*tool
}

func (r *registry) list() []toolDescriptor {
	out := make([]toolDescriptor, 0, len(r.tools))
	for _, t := range r.tools {
		out = append(out, toolDescriptor{
			Name:        t.name,
			Description: t.description,
			InputSchema: t.schema,
		})
	}
	return out
}

func (r *registry) byName(n string) (*tool, bool) {
	t, ok := r.index[n]
	return t, ok
}

// ---------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------

func decodeArgs(raw json.RawMessage, dst any) error {
	if len(raw) == 0 || string(raw) == "null" {
		// zero-arg tool call — leave dst at zero value
		return nil
	}
	return json.Unmarshal(raw, dst)
}

func idPath(prefix string, id int) string {
	return prefix + strconv.Itoa(id)
}

// ---------------------------------------------------------------------------
// Tool argument structs
// ---------------------------------------------------------------------------

type taskIDArgs struct {
	TaskID int `json:"task_id"`
}

type agentArgs struct {
	Agent string `json:"agent"`
}

type takeArgs struct {
	TaskID int    `json:"task_id"`
	Agent  string `json:"agent"`
}

type advanceArgs struct {
	TaskID  int    `json:"task_id"`
	Agent   string `json:"agent"`
	Approve bool   `json:"approve"`
}

type cancelArgs struct {
	TaskID int    `json:"task_id"`
	Agent  string `json:"agent"`
	Reason string `json:"reason"`
}

type reviewSubmitArgs struct {
	TaskID      int    `json:"task_id"`
	Agent       string `json:"agent"`
	Reviewer    string `json:"reviewer"`
	Verdict     string `json:"verdict"`
	Summary     string `json:"summary"`
	IsAggregate bool   `json:"is_aggregate"`
}

type updateArgs struct {
	TaskID  int               `json:"task_id"`
	Agent   string            `json:"agent"`
	Updates map[string]string `json:"updates"`
}

type searchArgs struct {
	Q     string `json:"q"`
	Limit int    `json:"limit"`
}

type nextArgs struct {
	Agent string `json:"agent"`
	Limit int    `json:"limit"`
}

type sessionCloseArgs struct {
	Agent        string `json:"agent"`
	SessionLabel string `json:"session_label"`
	DoneIDs      []int  `json:"done_ids"`
}

// ---------------------------------------------------------------------------
// Registry construction
// ---------------------------------------------------------------------------

func buildRegistry(c *toolClient) *registry {
	tools := []tool{
		{
			name:        "get_task",
			description: "Get full task details by ID",
			schema: objSchema(props{
				"task_id": intProp("Task ID"),
			}, "task_id"),
			handler: func(raw json.RawMessage) (any, error) {
				var a taskIDArgs
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				return c.callGET(idPath("/task/", a.TaskID))
			},
		},
		{
			name:        "task_history",
			description: "Get audit trail for a task (newest first)",
			schema: objSchema(props{
				"task_id": intProp("Task ID"),
			}, "task_id"),
			handler: func(raw json.RawMessage) (any, error) {
				var a taskIDArgs
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				return c.callGET(idPath("/task/", a.TaskID) + "/history")
			},
		},
		{
			name:        "list_tasks",
			description: "List tasks (optional owner/status filters)",
			schema: objSchema(props{
				"owner":  strProp("Filter by owner"),
				"status": strProp("Filter by status"),
			}),
			handler: func(raw json.RawMessage) (any, error) {
				var a struct {
					Owner  string `json:"owner"`
					Status string `json:"status"`
				}
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				q := url.Values{}
				if a.Owner != "" {
					q.Set("owner", a.Owner)
				}
				if a.Status != "" {
					q.Set("status", a.Status)
				}
				path := "/tasks"
				if e := q.Encode(); e != "" {
					path += "?" + e
				}
				return c.callGET(path)
			},
		},
		{
			name:        "search_tasks",
			description: "Full-text search across tasks (title, why, note)",
			schema: objSchema(props{
				"q":     strProp("Search query"),
				"limit": intProp("Max results (default 20)"),
			}, "q"),
			handler: func(raw json.RawMessage) (any, error) {
				var a searchArgs
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				q := url.Values{}
				q.Set("q", a.Q)
				if a.Limit > 0 {
					q.Set("limit", strconv.Itoa(a.Limit))
				}
				return c.callGET("/search?" + q.Encode())
			},
		},
		{
			name:        "status_counts",
			description: "Aggregate backlog counts by status",
			schema:      objSchema(props{}),
			handler: func(raw json.RawMessage) (any, error) {
				return c.callGET("/status")
			},
		},
		{
			name:        "analytics",
			description: "Backlog analytics (velocity, bottlenecks, time in status)",
			schema:      objSchema(props{}),
			handler: func(raw json.RawMessage) (any, error) {
				return c.callGET("/analytics")
			},
		},
		{
			name:        "next_tasks",
			description: "Recommend next tasks for an agent, ranked by score",
			schema: objSchema(props{
				"agent": strProp("Agent ID"),
				"limit": intProp("Max results (default 5)"),
			}, "agent"),
			handler: func(raw json.RawMessage) (any, error) {
				var a nextArgs
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				path := "/next/" + url.PathEscape(a.Agent)
				if a.Limit > 0 {
					path += "?limit=" + strconv.Itoa(a.Limit)
				}
				return c.callGET(path)
			},
		},
		{
			name:        "take_task",
			description: "Take a task (READY → IN_PROGRESS)",
			schema: objSchema(props{
				"task_id": intProp("Task ID"),
				"agent":   strProp("Taking agent"),
			}, "task_id", "agent"),
			handler: func(raw json.RawMessage) (any, error) {
				var a takeArgs
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				return c.callPOST(idPath("/task/", a.TaskID)+"/take", map[string]string{"agent": a.Agent})
			},
		},
		{
			name:        "release_task",
			description: "Release a task (IN_PROGRESS → READY)",
			schema: objSchema(props{
				"task_id": intProp("Task ID"),
				"agent":   strProp("Releasing agent"),
			}, "task_id", "agent"),
			handler: func(raw json.RawMessage) (any, error) {
				var a takeArgs
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				return c.callPOST(idPath("/task/", a.TaskID)+"/release", map[string]string{"agent": a.Agent})
			},
		},
		{
			name:        "advance_task",
			description: "Advance a task to the next status (gate-checked; --approve for AWAITING_APPROVAL→DONE)",
			schema: objSchema(props{
				"task_id": intProp("Task ID"),
				"agent":   strProp("Advancing agent"),
				"approve": boolProp("Approve final gate (AWAITING_APPROVAL→DONE)"),
			}, "task_id", "agent"),
			handler: func(raw json.RawMessage) (any, error) {
				var a advanceArgs
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				body := map[string]any{"agent": a.Agent}
				if a.Approve {
					body["approve"] = true
				}
				return c.callPOST(idPath("/task/", a.TaskID)+"/advance", body)
			},
		},
		{
			name:        "cancel_task",
			description: "Cancel a task (any non-terminal → CANCELLED)",
			schema: objSchema(props{
				"task_id": intProp("Task ID"),
				"agent":   strProp("Cancelling agent"),
				"reason":  strProp("Explanation"),
			}, "task_id", "agent"),
			handler: func(raw json.RawMessage) (any, error) {
				var a cancelArgs
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				return c.callPOST(idPath("/task/", a.TaskID)+"/cancel", map[string]string{
					"agent":  a.Agent,
					"reason": a.Reason,
				})
			},
		},
		{
			name:        "review_submit",
			description: "Record a reviewer verdict on a task (PASS/NEEDS_WORK/FAIL)",
			schema: objSchema(props{
				"task_id":      intProp("Task ID"),
				"agent":        strProp("Caller (audit_trail.agent)"),
				"reviewer":     strProp("Reviewer model (e.g. code-reviewer)"),
				"verdict":      strProp("Verdict (PASS/NEEDS_WORK/FAIL)"),
				"summary":      strProp("Optional summary"),
				"is_aggregate": boolProp("Aggregate parent review"),
			}, "task_id", "agent", "reviewer", "verdict"),
			handler: func(raw json.RawMessage) (any, error) {
				var a reviewSubmitArgs
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				return c.callPOST(idPath("/task/", a.TaskID)+"/review-submit", map[string]any{
					"agent":        a.Agent,
					"reviewer":     a.Reviewer,
					"verdict":      a.Verdict,
					"summary":      a.Summary,
					"is_aggregate": a.IsAggregate,
				})
			},
		},
		{
			name:        "update_task",
			description: "Update task safe-fields (title/why/note/mode/...)",
			schema: objSchema(props{
				"task_id": intProp("Task ID"),
				"agent":   strProp("Updating agent"),
				"updates": mapProp("Field:value map"),
			}, "task_id", "agent", "updates"),
			handler: func(raw json.RawMessage) (any, error) {
				var a updateArgs
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				return c.callPATCH(idPath("/task/", a.TaskID), map[string]any{
					"agent":   a.Agent,
					"updates": a.Updates,
				})
			},
		},
		{
			name:        "session_close",
			description: "Record session_close audit (one row per DONE task)",
			schema: objSchema(props{
				"agent":         strProp("Closing agent"),
				"session_label": strProp("Session label (e.g. samvel-34)"),
				"done_ids":      intArrProp("Task IDs marked DONE in this session"),
			}, "agent", "session_label"),
			handler: func(raw json.RawMessage) (any, error) {
				var a sessionCloseArgs
				if err := decodeArgs(raw, &a); err != nil {
					return nil, err
				}
				return c.callPOST("/session/close", map[string]any{
					"agent":         a.Agent,
					"session_label": a.SessionLabel,
					"done_ids":      a.DoneIDs,
				})
			},
		},
	}

	r := &registry{tools: tools, index: make(map[string]*tool, len(tools))}
	for i := range r.tools {
		r.index[r.tools[i].name] = &r.tools[i]
	}
	return r
}

// ---------------------------------------------------------------------------
// JSON Schema helpers (Draft 7 subset used by MCP tool inputSchema)
// ---------------------------------------------------------------------------

type props map[string]map[string]any

func objSchema(p props, required ...string) map[string]any {
	out := map[string]any{
		"type":       "object",
		"properties": map[string]any(p.toMap()),
	}
	if len(required) > 0 {
		out["required"] = required
	}
	return out
}

func (p props) toMap() map[string]any {
	m := make(map[string]any, len(p))
	for k, v := range p {
		m[k] = v
	}
	return m
}

func strProp(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}

func intProp(desc string) map[string]any {
	return map[string]any{"type": "integer", "description": desc}
}

func boolProp(desc string) map[string]any {
	return map[string]any{"type": "boolean", "description": desc}
}

func mapProp(desc string) map[string]any {
	return map[string]any{
		"type":                 "object",
		"description":          desc,
		"additionalProperties": map[string]any{"type": "string"},
	}
}

func intArrProp(desc string) map[string]any {
	return map[string]any{
		"type":        "array",
		"description": desc,
		"items":       map[string]any{"type": "integer"},
	}
}
