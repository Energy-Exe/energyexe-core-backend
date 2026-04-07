# Brain Agent — Requirements & Architecture

## 1. What It Is

An AI-powered data analyst embedded in the EnergyExe admin dashboard. Users ask questions about energy data (generation, prices, capacity factors) and the agent queries the database, generates charts, and reads source code to provide data-backed answers.

## 2. Core Requirements

### 2.1 Conversation

| Requirement | Detail |
|-------------|--------|
| Multi-turn | Agent remembers everything said in the current thread |
| Thread persistence | Conversations saved to DB, loadable later |
| No duplicate messages | Each user/assistant message appears exactly once |
| No lost messages | If connection drops, no data is lost |
| Source of truth | SDK session transcript on disk → DB → Frontend |

### 2.2 Real-Time UX

| Requirement | Detail |
|-------------|--------|
| Streaming text | User sees agent's text as it's generated, character by character |
| Tool visibility | User sees which tools the agent is calling (Bash, Read, Grep) |
| Status indicators | Thinking, executing, analyzing phases visible |
| Connection resilience | Works through Railway proxy; handles drops gracefully |

### 2.3 Data Access

| Requirement | Detail |
|-------------|--------|
| Database queries | Via `db.py` helper script in sandbox |
| Chart generation | Matplotlib PNGs, displayed in chat |
| Source code | Read-only access to all 3 repos via Read/Glob/Grep tools |
| Web search | WebSearch and WebFetch for external data |

### 2.4 Session Management

| Requirement | Detail |
|-------------|--------|
| Session lifecycle | Created on first message, reused across turns, cleaned up after 30min idle |
| Model selection | Sonnet 4.6 (default) or Opus 4.6, switchable per thread |
| Conversation continuity | When session expires, context is restored from history |
| Turn limits | Max 50 turns per query to prevent runaway loops |

## 3. Architecture

### 3.1 Data Flow

```
User sends message
  → Frontend adds user message + placeholder assistant message to UI
  → POST /brain-agent/chat (prompt, session_id)
  → Backend streams SSE events for real-time UX
  → Agent processes (queries DB, reads files, thinks)
  → ResultMessage signals turn complete
  → Backend reads authoritative conversation from SDK transcript
  → Backend saves to DB (agent_threads table)
  → SSE result event includes final messages
  → Frontend replaces placeholder with authoritative messages
  → Thread saved/updated in DB
```

### 3.2 Source of Truth Chain

```
1. SDK session transcript (JSONL on disk) — written by Claude Code subprocess
2. Database (agent_threads.messages) — saved after each turn from SDK transcript
3. Frontend state — rendered from DB data, enhanced with real-time SSE
```

**Rule**: Frontend NEVER overwrites DB state. DB is always populated from SDK transcripts. Frontend reads from DB.

### 3.3 SSE Event Flow

SSE is used for **real-time UX only** — not for building authoritative state.

Events streamed during processing:
- `session` — session ID assigned
- `status` — thinking/tool/analyzing/responding
- `text_delta` — incremental text for typing effect
- `tool_use` — tool invocation started
- `tool_result` — tool execution completed
- `image` — chart generated
- `result` — turn complete, includes authoritative messages from SDK

### 3.4 After Turn Completes (ResultMessage)

```python
from claude_agent_sdk import get_session_messages

# Read authoritative conversation from SDK's disk transcript
sdk_messages = get_session_messages(session_id, directory=work_dir)

# Convert to our format and save to DB
thread.messages = convert_sdk_messages(sdk_messages)
await db.commit()

# Include in result SSE event
yield SSEEvent("result", {"messages": thread.messages, ...})
```

### 3.5 Frontend Receives Result

```typescript
onResult: (meta) => {
  if (meta.messages) {
    // Replace event-built state with authoritative SDK state
    setMessages(meta.messages)
  }
  // Save thread to server (optional, backend already saved)
}
```

### 3.6 Conversation Continuity

When a session expires and is recreated:
1. Frontend sends `conversation_history` (last 20 messages) with the request
2. Backend detects new session, prepends history to prompt as context
3. SDK creates a fresh session but the agent has full context via the prompt

## 4. What NOT To Do

- Do NOT build authoritative message state from SSE events on the frontend
- Do NOT save from frontend to DB (backend saves from SDK transcript)
- Do NOT poll the DB during streaming
- Do NOT use progressive saves during agent processing
- Do NOT rely on SSE delivery for correctness — SSE is UX-only

## 5. Components

### Backend
- `app/services/brain_agent_service.py` — orchestrates SDK sessions, streams SSE, reads transcripts
- `app/api/v1/endpoints/brain_agent.py` — HTTP endpoints, SSE streaming
- `app/models/agent_thread.py` — DB model for thread persistence
- `app/schemas/brain_agent.py` — request/response schemas
- `app/prompts/brain_agent_system.md` — agent system prompt
- `app/services/brain_agent_repo_manager.py` — source code access

### Frontend
- `src/hooks/use-brain-agent.ts` — conversation state, session management
- `src/lib/brain-agent-api.ts` — SSE streaming client
- `src/lib/brain-agent-threads-api.ts` — thread CRUD
- `src/components/brain-agent/` — UI components
- `src/types/brain-agent.ts` — TypeScript types
