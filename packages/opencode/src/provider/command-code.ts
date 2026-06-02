import path from "path"
import os from "os"
import fs from "fs"
import { spawn } from "child_process"

const CMDC_QUEUE: Array<() => void> = []
let CMDC_RUNNING = false

function cmdcQueue<T>(fn: () => Promise<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    CMDC_QUEUE.push(async () => {
      try { resolve(await fn()) }
      catch (e) { reject(e) }
    })
    if (!CMDC_RUNNING) cmdcDrain()
  })
}

function cmdcDrain() {
  if (CMDC_QUEUE.length === 0) { CMDC_RUNNING = false; return }
  CMDC_RUNNING = true
  const next = CMDC_QUEUE.shift()!
  Promise.resolve(next()).finally(() => cmdcDrain())
}

export function readCommandCodeAuth(): string | undefined {
  const envKey = process.env["COMMAND_CODE_API_KEY"]
  if (envKey) return envKey
  try {
    const data = JSON.parse(fs.readFileSync(path.join(os.homedir(), ".commandcode", "auth.json"), "utf-8"))
    if (typeof data.apiKey === "string" && data.apiKey) return data.apiKey
  } catch {}
  return undefined
}

function cmdcCommand(): string {
  return process.platform === "win32" ? "cmdc.cmd" : "cmdc"
}

function extractTextFromContent(content: any): string {
  if (typeof content === "string") return content
  if (Array.isArray(content)) {
    return content
      .filter((part: any) => part.type === "text")
      .map((part: any) => part.text || "")
      .join("\n")
  }
  return ""
}

export function buildPrompt(messages: Array<{ role: string; content: any }>): string {
  const parts = messages.map((msg) => {
    const text = extractTextFromContent(msg.content)
    if (!text) return null
    const label = msg.role === "system" ? "System" : msg.role === "user" ? "User" : msg.role === "assistant" ? "Assistant" : msg.role
    return `${label}: ${text}`
  }).filter(Boolean).join("\n")
  return parts + "\nAssistant: "
}

export async function spawnCmdc(
  model: string,
  prompt: string,
  signal?: AbortSignal,
): Promise<string> {
  return cmdcQueue(() =>
    new Promise<string>((resolve, reject) => {
      const cmd = cmdcCommand()
      const args = ["--print", "--model", model, "--yolo", "--skip-onboarding", "--max-turns", "1"]
      const isWin = process.platform === "win32"

      const proc = isWin
        ? spawn("cmd", ["/c", cmd, ...args], { stdio: ["pipe", "pipe", "pipe"], windowsHide: true })
        : spawn(cmd, args, { stdio: ["pipe", "pipe", "pipe"] })

      const onAbort = () => { proc.kill(); reject(new Error("Request aborted")) }
      if (signal) {
        if (signal.aborted) return reject(new Error("Request aborted"))
        signal.addEventListener("abort", onAbort, { once: true })
      }

      const stdout: Buffer[] = []
      const stderr: Buffer[] = []

      proc.stdout.on("data", (chunk: Buffer) => stdout.push(chunk))
      proc.stderr.on("data", (chunk: Buffer) => stderr.push(chunk))

      const done = () => {
        if (signal) signal.removeEventListener("abort", onAbort)
      }

      proc.on("error", (err: Error) => { done(); reject(err) })

      proc.on("close", (code: number | null) => {
        done()
        if (code !== 0 && stdout.length === 0) {
          reject(new Error(Buffer.concat(stderr).toString("utf-8").trim() || `cmdc exited with code ${code}`))
          return
        }
        resolve(Buffer.concat(stdout).toString("utf-8").trim())
      })

      proc.stdin.write(prompt)
      proc.stdin.end()
    }),
  )
}

function formatId() {
  return "chatcmpl-" + Math.random().toString(36).slice(2, 11)
}

export function formatNonStreamingResponse(text: string, model: string): string {
  return JSON.stringify({
    id: formatId(),
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{ index: 0, message: { role: "assistant", content: text }, finish_reason: "stop" }],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  })
}

export function formatStreamingResponse(text: string, model: string): string {
  const id = formatId()
  const created = Math.floor(Date.now() / 1000)
  const chunks = [
    { id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] },
    { id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { content: text }, finish_reason: null }] },
    { id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: "stop" }] },
  ]
  return chunks.map((c) => `data: ${JSON.stringify(c)}\n`).join("") + "data: [DONE]\n"
}

export async function commandCodeFetch(
  originalFetch: typeof fetch,
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<Response> {
  const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url
  const isChat = (url.includes("/chat/completions") || url.includes("/v1/responses")) && init?.method !== "GET"
  if (!isChat) return originalFetch(input, init)

  let res: Response
  try { res = await originalFetch(input, init) }
  catch { return originalFetch(input, init) }

  if (res.status !== 403) return res

  let bodyText = ""
  try { bodyText = await res.clone().text() } catch {}
  if (!bodyText.includes("upgrade")) return res

  let requestBody: any
  try { requestBody = JSON.parse(typeof init?.body === "string" ? init.body : "{}") }
  catch { return res }

  const model = requestBody.model || "deepseek/deepseek-v4-flash"
  const messages = requestBody.messages ?? []
  const stream = requestBody.stream ?? false

  try {
    const cliOutput = await spawnCmdc(model, buildPrompt(messages))
    const body = stream ? formatStreamingResponse(cliOutput, model) : formatNonStreamingResponse(cliOutput, model)
    return new Response(body, {
      status: 200,
      headers: { "content-type": stream ? "text/event-stream" : "application/json" },
    })
  } catch (err) {
    return new Response(JSON.stringify({
      error: { message: `Command Code CLI fallback failed: ${err}`, type: "server_error" },
    }), { status: 502, headers: { "content-type": "application/json" } })
  }
}
