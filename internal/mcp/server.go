// Package mcp implements a Model Context Protocol (MCP) server as a
// stdio sidecar. It bridges Claude Code (MCP client) to backlog-server's
// REST API — a thin translation layer from JSON-RPC 2.0 tool calls to
// HTTP requests against $BACKLOG_SERVER_URL.
//
// Protocol: MCP 2024-11-05 subset (initialize / tools/list / tools/call).
// Transport: JSON-RPC 2.0 over stdio (newline-delimited JSON).
//
// Task #1431 (Phase 4 native REST migration).
package mcp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const protocolVersion = "2024-11-05"

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type initializeResult struct {
	ProtocolVersion string            `json:"protocolVersion"`
	Capabilities    initCapabilities  `json:"capabilities"`
	ServerInfo      map[string]string `json:"serverInfo"`
}

type initCapabilities struct {
	Tools map[string]any `json:"tools"`
}

type toolsListResult struct {
	Tools []toolDescriptor `json:"tools"`
}

type toolDescriptor struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

type toolCallResult struct {
	Content []toolContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

type toolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type callParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// Run reads MCP JSON-RPC requests from stdin, dispatches to tool handlers
// via toolClient (HTTP wrapper around backlog-server REST), and writes
// responses to stdout. Server-side logs go to stderr. Blocks until stdin
// closes (EOF) or an unrecoverable read error occurs.
func Run(baseURL, agentKey string) error {
	if baseURL == "" {
		return fmt.Errorf("BACKLOG_SERVER_URL required for mcp sidecar")
	}
	if agentKey == "" {
		return fmt.Errorf("BACKLOG_AGENT_KEY required for mcp sidecar")
	}
	tc := newToolClient(baseURL, agentKey)
	registry := buildRegistry(tc)

	fmt.Fprintln(os.Stderr, "backlog-server MCP sidecar started (stdio, protocol "+protocolVersion+")")

	reader := bufio.NewReader(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			if line == "" {
				return nil
			}
			// process trailing line without newline, then exit
			if resp := handleLine(line, registry); resp != nil {
				writeResponse(writer, resp)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("stdin read: %w", err)
		}
		if resp := handleLine(line, registry); resp != nil {
			writeResponse(writer, resp)
		}
	}
}

func writeResponse(w *bufio.Writer, resp *rpcResponse) {
	b, err := json.Marshal(resp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mcp: marshal response failed: %v\n", err)
		return
	}
	if _, err := w.Write(b); err != nil {
		fmt.Fprintf(os.Stderr, "mcp: write response failed: %v\n", err)
		return
	}
	if err := w.WriteByte('\n'); err != nil {
		fmt.Fprintf(os.Stderr, "mcp: write newline failed: %v\n", err)
		return
	}
	if err := w.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "mcp: flush failed: %v\n", err)
	}
}

// handleLine parses one JSON-RPC line and returns a response (or nil for
// notifications which don't carry an id and must not be replied to).
func handleLine(line string, reg *registry) *rpcResponse {
	var req rpcRequest
	if err := json.Unmarshal([]byte(line), &req); err != nil {
		return &rpcResponse{
			JSONRPC: "2.0",
			Error:   &rpcError{Code: -32700, Message: "parse error: " + err.Error()},
		}
	}

	// Notifications carry no id and receive no response per JSON-RPC 2.0.
	isNotification := len(req.ID) == 0

	switch req.Method {
	case "initialize":
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: initializeResult{
				ProtocolVersion: protocolVersion,
				Capabilities:    initCapabilities{Tools: map[string]any{}},
				ServerInfo:      map[string]string{"name": "backlog-server", "version": "1.0.0"},
			},
		}
	case "notifications/initialized":
		return nil // notification, no reply
	case "tools/list":
		descs := reg.list()
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  toolsListResult{Tools: descs},
		}
	case "tools/call":
		return handleToolCall(reg, req)
	case "ping":
		return &rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: map[string]any{}}
	default:
		if isNotification {
			return nil
		}
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &rpcError{Code: -32601, Message: "method not found: " + req.Method},
		}
	}
}

func handleToolCall(reg *registry, req rpcRequest) *rpcResponse {
	var p callParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &rpcError{Code: -32602, Message: "invalid params: " + err.Error()},
		}
	}
	tool, ok := reg.byName(p.Name)
	if !ok {
		return errorToolResponse(req.ID, "unknown tool: "+p.Name)
	}
	result, err := tool.handler(p.Arguments)
	if err != nil {
		return errorToolResponse(req.ID, err.Error())
	}
	// Encode result as compact JSON text (MCP tool responses use content array).
	body, mErr := json.Marshal(result)
	if mErr != nil {
		return errorToolResponse(req.ID, "marshal tool result: "+mErr.Error())
	}
	return &rpcResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: toolCallResult{
			Content: []toolContent{{Type: "text", Text: string(body)}},
		},
	}
}

func errorToolResponse(id json.RawMessage, msg string) *rpcResponse {
	body, _ := json.Marshal(map[string]string{"error": msg})
	return &rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result: toolCallResult{
			Content: []toolContent{{Type: "text", Text: string(body)}},
			IsError: true,
		},
	}
}
