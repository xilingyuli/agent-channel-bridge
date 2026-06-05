#!/usr/bin/env node
/**
 * ACP ↔ Claude Agent SDK (→ DeepSeek API)
 * v5.0 — Uses @anthropic-ai/claude-agent-sdk for full Claude Code capabilities
 *        including AGENTS.md, SOUL.md, skills auto-loading.
 */

import { query } from '@anthropic-ai/claude-agent-sdk';
import { createInterface } from 'node:readline';
import { randomUUID } from 'node:crypto';

// ── Config ──────────────────────────────────────────────────────────
const API_KEY = process.env.ANTHROPIC_API_KEY || '';
const BASE_URL = process.env.ANTHROPIC_BASE_URL || 'https://api.deepseek.com/anthropic';
const MODEL = process.env.CLAUDE_MODEL || 'claude-sonnet-4-6';
const MAX_TURNS = parseInt(process.env.CLAUDE_MAX_TURNS || '5', 10);

// ── State ───────────────────────────────────────────────────────────
const sessions = new Map();  // bridge session id → { sdkSessionId, history: [] }
const running = new Set();

// ── Helpers ─────────────────────────────────────────────────────────
function stderr(msg) {
  process.stderr.write(`[claude-sdk] ${msg}\n`);
}

function stdout(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ── ACP Handlers ────────────────────────────────────────────────────
async function handleInit(id) {
  stdout({
    jsonrpc: '2.0', id,
    result: {
      protocolVersion: 6,
      capabilities: {},
      serverInfo: { name: 'claude-agent-sdk', title: 'Claude Code via Agent SDK → DeepSeek', version: '5.0' }
    }
  });
}

async function handleNew(id, params) {
  const sid = randomUUID();
  const cwd = params?.cwd || process.cwd();
  sessions.set(sid, { sdkSessionId: null, cwd, history: [] });
  stdout({ jsonrpc: '2.0', id, result: { sessionId: sid } });
}

async function handlePrompt(id, params) {
  const sid = params?.sessionId || '';
  const prompts = params?.prompt || [];
  const session = sessions.get(sid);

  if (!session) {
    stdout({ jsonrpc: '2.0', id, error: { code: -32603, message: `Session not found: ${sid}` } });
    return;
  }

  running.add(sid);
  const text = prompts.filter(p => p.type === 'text').map(p => p.text).join('\n');
  session.history.push({ role: 'user', content: text });

  try {
    const opts = {
      cwd: session.cwd,
      model: MODEL,
      maxTurns: MAX_TURNS,
      permissionMode: 'bypassPermissions',
      allowDangerouslySkipPermissions: true,
      settingSources: ['user', 'project', 'local'],
      skills: 'all',
    };

    // Resume previous session if exists
    if (session.sdkSessionId) {
      opts.resume = session.sdkSessionId;
    }

    const q = query({ prompt: text, options: opts });
    let fullResponse = '';

    for await (const msg of q) {
      switch (msg.type) {
        case 'stream_event': {
          // Streaming text chunks (may not fire with all providers)
          if (msg.event?.type === 'content_block_delta' &&
              msg.event.delta?.type === 'text_delta') {
            const chunk = msg.event.delta.text;
            fullResponse += chunk;
            stdout({
              jsonrpc: '2.0',
              method: 'session/update',
              params: {
                sessionId: sid,
                update: {
                  sessionUpdate: 'agent_message_chunk',
                  content: { type: 'text', text: chunk }
                }
              }
            });
          }
          break;
        }

        case 'assistant': {
          // Capture session_id for future resumes
          if (msg.session_id && !session.sdkSessionId) {
            session.sdkSessionId = msg.session_id;
          }
          // Extract text from content blocks (primary delivery path for some models)
          if (Array.isArray(msg.message?.content)) {
            for (const block of msg.message.content) {
              if (block.type === 'text' && block.text) {
                fullResponse += block.text;
                stdout({
                  jsonrpc: '2.0',
                  method: 'session/update',
                  params: {
                    sessionId: sid,
                    update: {
                      sessionUpdate: 'agent_message_chunk',
                      content: { type: 'text', text: block.text }
                    }
                  }
                });
              }
            }
          }
          break;
        }

        case 'result': {
          // Final result
          if (msg.session_id) {
            session.sdkSessionId = msg.session_id;
          }

          // Save assistant response to history
          if (fullResponse.trim()) {
            session.history.push({ role: 'assistant', content: fullResponse });
          }

          // Trim history (keep last 10 rounds)
          const maxEntries = 20;
          if (session.history.length > maxEntries) {
            session.history = session.history.slice(-maxEntries);
          }

          stdout({
            jsonrpc: '2.0', id,
            result: {
              stopReason: msg.stop_reason || 'end_turn',
              usage: msg.usage || {},
              cost: msg.total_cost_usd || 0,
            }
          });
          break;
        }
      }
    }
  } catch (err) {
    stderr(`prompt error: ${err.message}`);
    stdout({
      jsonrpc: '2.0',
      method: 'session/update',
      params: {
        sessionId: sid,
        update: {
          sessionUpdate: 'agent_message_chunk',
          content: { type: 'text', text: `❌ ${err.message}` }
        }
      }
    });
    stdout({ jsonrpc: '2.0', id, error: { code: -32603, message: err.message } });
  } finally {
    running.delete(sid);
  }
}

async function handleClose(id, params) {
  const sid = params?.sessionId || '';
  sessions.delete(sid);
  running.delete(sid);
  stdout({ jsonrpc: '2.0', id, result: {} });
}

async function handleResume(id, params) {
  const sid = params?.sessionId || '';
  if (sessions.has(sid)) {
    stdout({ jsonrpc: '2.0', id, result: { sessionId: sid } });
  } else {
    await handleNew(id, params);
  }
}

async function handlePerm(id) {
  stdout({
    jsonrpc: '2.0', id,
    result: { option: { optionId: 'once' }, outcome: 'allow_once' }
  });
}

// ── Main ────────────────────────────────────────────────────────────
async function main() {
  stderr(`v5.0 start — ${BASE_URL} — model=${MODEL} — max_turns=${MAX_TURNS}`);

  const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    let msg;
    try { msg = JSON.parse(trimmed); } catch { continue; }

    const { id, method, params } = msg;
    const handlers = {
      'initialize': handleInit,
      'session/new': handleNew,
      'session/prompt': handlePrompt,
      'session/close': handleClose,
      'session/resume': handleResume,
      'session/request_permission': handlePerm,
    };

    const handler = handlers[method];
    if (handler) {
      handler(id, params || {}).catch(err => {
        stderr(`dispatch error [${method}]: ${err.message}`);
      });
    } else {
      stdout({
        jsonrpc: '2.0', id,
        error: { code: -32601, message: `Unknown: ${method}` }
      });
    }
  }
}

main().catch(err => {
  stderr(`fatal: ${err.message}`);
  process.exit(1);
});
