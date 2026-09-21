# zio-bedrock

[![javadocs.dev](https://www.javadocs.dev/com.jamesward/zio-bedrock_3/badge.svg)](https://www.javadocs.dev/com.jamesward/zio-bedrock_3/latest)

A Scala 3 / ZIO library with one typed API for two Amazon Bedrock backends:

- the native [Converse and ConverseStream APIs](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Converse.html) at
  `https://bedrock-runtime.<region>.amazonaws.com/model/<model-id>/converse`; and
- the OpenAI-compatible [Chat Completions API on the `bedrock-mantle` endpoint](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-chat-completions-mantle.html) at
  `https://bedrock-mantle.<region>.api.aws/v1/chat/completions`.

Both use a Bedrock API key (bearer token; this library does not implement SigV4).
The public API keeps a ZIO-native, typed model rather than exposing backend DTOs.
At the HTTP boundary, private transports translate messages, function tools,
structured output, usage, stop reasons, reasoning/tool events, and streaming.

- Typed tool APIs derive input / output / error JSON Schemas from
  `zio.schema.Schema`; runtime catalogs can supply JSON Schema objects
  directly with `Tool.dynamic`.
- Built on ZIO HTTP's `Client`.
- Four APIs:
  - **`Bedrock.chat`** — low-level. You drive the wire, including
    manual tool dispatch.
  - **`Bedrock.request`** — high-level single-turn. Bundle typed handlers in a
    NamedTuple, then fold over the typed outcome. Tool errors flow through
    ZIO's error channel as a typed union.
  - **`Bedrock.loop`** — typed multi-turn agentic loop. The framework dispatches
    a handler NamedTuple and feeds results back automatically.
  - **`Bedrock.dynamicLoop`** — runtime multi-turn loop. Supply tools discovered
    at runtime plus one generic handler; the library owns complete conversation
    history and returns per-turn and aggregate usage/latency metrics.

## Install

```scala
libraryDependencies += "com.jamesward" %% "zio-bedrock" % "<version>"
```

## Configure

Choose either backend layer; both provide the shared `Bedrock` service.

### Mantle

`Mantle.configured` reads the following environment variables and requires an existing
`zio.http.Client`; `Mantle.live` is self-contained and includes `Client.default`:

| Var                         | Required | Default     |
| --------------------------- | -------- | ----------- |
| `AWS_BEARER_TOKEN_BEDROCK`  | yes      | —           |
| `BEDROCK_MODEL_ID`          | yes      | —           |
| `AWS_REGION`                | no       | `us-east-1` |

For example:
1. [Create a Bedrock API key](https://docs.aws.amazon.com/bedrock/latest/userguide/api-keys.html).
2. Set the auth token: `export AWS_BEARER_TOKEN_BEDROCK=YOUR_TOKEN`.
3. Set a model returned by Mantle's `GET /v1/models`, for example
   `export BEDROCK_MODEL_ID=openai.gpt-oss-120b`.

Or construct the regional layer explicitly:

```scala
import com.jamesward.zio_bedrock.{Bedrock, Mantle, MantleConfig}
import com.jamesward.zio_bedrock.Bedrock.*

Mantle.layer(MantleConfig(
  ApiKey("…"),
  Region.UsEast1,
  ModelId("openai.gpt-oss-120b"),
))
```

A custom origin is also supported for a Mantle-compatible local service such
as [acprock](https://github.com/jamesward/acprock); the client still posts to
`/v1/chat/completions`:

```scala
import zio.http.URL

Mantle.layer(MantleConfig(
  ApiKey("dev"),
  URL.decode("http://localhost:9999").toOption.get,
  ModelId("anthropic.claude-opus-4-7"),
))
```

### Converse

`Converse.configured` requires `AWS_BEARER_TOKEN_BEDROCK` and
`BEDROCK_MODEL_ID`; it reads `AWS_REGION` with the same `us-east-1` default.
Both configured layers fail with a typed `InvalidRegion` for a non-empty
unsupported region. `Converse.live` includes `Client.default`.

```scala
import com.jamesward.zio_bedrock.{Bedrock, Converse, ConverseConfig}
import com.jamesward.zio_bedrock.Bedrock.*

Converse.layer(ConverseConfig(
  ApiKey("…"),
  Region.UsEast1,
  ModelId("anthropic.claude-sonnet-4-5-v2:0"),
))
```

For local protocol tests or a compatible service, pass a custom origin; model
path encoding and `/converse` or `/converse-stream` are still supplied by the
transport:

```scala
Converse.layer(ConverseConfig(
  ApiKey("dev"),
  URL.decode("http://localhost:9999").toOption.get,
  ModelId("local:model"),
))
```

## Basic inference (`Bedrock.chat`)

Five terminals, one prompt each. Every example below assumes either
`Mantle.live` / `Converse.live` (or `Client.default` plus the corresponding
`configured` layer) in scope.

```scala
case class Food(name: String, region: String) derives Schema

// Plain text.
Bedrock.chat("say hello").text

// Structured output: the model is told to produce JSON conforming to
// Schema[Food], and the text reply is decoded into a Food.
Bedrock.chat("Favorite food").as[Food]

// Full envelope: stopReason, usage, metrics, plus the assistant message.
Bedrock.chat("tell a one-line joke").asResponse

// Structured output + envelope.
Bedrock.chat("Worst food").asResponse[Food]

// Streaming: each emitted String is a text delta from the model.
Bedrock.chat("Write a poem about Scala").textStream
  .runForeach(Console.print(_).orDie)
```


`RequestConfig(prompt: String)` is a single-message convenience —
`Bedrock.chat("hi")` is just `Bedrock.chat(RequestConfig("hi"))`.
For full control over messages, system prompt, and inference config,
pass a `RequestConfig` directly.

A structured-output decode failure surfaces as
`Bedrock.Error.StructuredDecode(responseText, message)`.

`asStream` exposes normalized delta, stop, and metadata events and always ends
with exactly one `StreamEvent.Complete`, whose `result` contains the complete
public model turn. Shared streaming loops suppress intermediate completion
markers and retain exact backend continuation blocks such as Converse reasoning
signatures.

Messages returned by a backend privately retain the exact normalized assistant
turn. This preserves signed Converse reasoning and other continuation data when
`first.output.message` is appended to a follow-up `RequestConfig`; private data
is redacted from rendering and equality. Call `message.portable` to explicitly
discard backend continuation data and keep only public text/tool blocks.

## Defining typed tools

Tool handlers are bundled in a `NamedTuple`. The key becomes the tool
name advertised to the model; input/output/error types must have
`Schema` instances.

```scala
val tools = (
  // Effectful: ZIO[R, E, A]. Schema[E] is required so loop can wire-encode
  // failures back to the model.
  randomLetters = ToolHandler(
    (n: Int) => Random.nextIntBounded(26).replicateZIO(n)
      .map(_.map(i => ('a' + i).toChar).mkString),
    "generate n random letters",
  ),
  // Pure: I => A. No environment, no errors.
  reverse = ToolHandler.fromPure(
    (s: String) => s.reverse,
    "reverse a string",
  ),
)
```

The same `tools` NamedTuple drives both `Bedrock.loop` and
`Bedrock.request`.

For input/output classes you can `derives Schema` and field
descriptions propagate to the JSON Schema sent to the model.

## Multi-turn agentic loop (`Bedrock.loop`)

The framework dispatches tools and feeds results back to the model
until the model produces a final reply (or `maxIterations` is hit,
default 10). Handler errors are encoded via `Schema[E]` and fed back
to the model as `tool_result.status = Error` — they don't surface in
the ZIO error channel.

```scala
// Final text reply.
Bedrock.loop("generate 8 random letters", tools).text

// Final reply parsed as Food.
Bedrock.loop("generate 8 random letters; suggest a similar food name", tools)
  .as[Food]

// Multiple tool calls per loop, streaming the final reply's text.
Bedrock.loop("display 8 random letters and its reverse", tools)
  .textStream.runForeach(Console.print(_).orDie)
```

Configuration:

```scala
Bedrock.loop("…", tools)
  .system("You are concise.")
  .inferenceConfig(InferenceConfig(maxTokens = 500))
  .maxIterations(5)
  .text
```

Debug logging is built in at `ZIO.logDebug` level — set your ZIO log
level to `DEBUG` to see each iteration's tool dispatches and replies.

When one model turn emits multiple tool calls, loop handlers run concurrently
with a maximum parallelism of 8. Tool results are returned to the model in the
original tool-use order. Handlers that share mutable state or perform ordered
side effects must provide their own synchronization.

## Runtime tools and metrics (`Bedrock.dynamicLoop`)

Use `dynamicLoop` when tool names and input schemas are discovered at
runtime rather than represented by a compile-time NamedTuple. A
`Tool.dynamic` carries its `zio.json.ast.Json.Obj` input schema verbatim;
`ToolInput.asJsonObject` exposes the model-generated arguments without a
Scala input class.

The following adapter uses
[`zio-http-mcp`](https://github.com/jamesward/zio-http-mcp). It has no
hard-coded tool names or schemas: every tool comes from MCP `tools/list`,
and one generic handler dispatches every call.

```scala
import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.*
import com.jamesward.ziohttp.mcp.client.McpClient
import zio.*
import zio.json.*

// Requires "com.jamesward" %% "zio-http-mcp" % "<version>".
def runWithMcp(mcp: McpClient): ZIO[Bedrock, Throwable, DynamicLoopResult[String]] =
  for
    definitions <- mcp.listTools
    tools = definitions.toList.map: definition =>
      Tool.dynamic(
        ToolName(definition.name.value),
        definition.description.getOrElse(definition.name.value),
        definition.inputSchema,
      )
    available = definitions.map(_.name.value).toSet
    result <- Bedrock.dynamicLoop("answer using the available tools", tools)((name, input) =>
      val toolName = name.unwrap
      for
        _ <- ZIO.fail(new IllegalArgumentException(s"unknown tool: $toolName"))
          .unless(available.contains(toolName))
        arguments <- ZIO.fromEither(input.asJsonObject)
          .mapError(message => new IllegalArgumentException(message))
        response <- mcp.callTool(toolName, arguments)
        content = response.content.toJson
        _ <- ZIO.fail(new RuntimeException(content))
          .when(response.isError.contains(true))
      yield DynamicToolResult.text(content)
    )
    .maxIterations(20)
    .text
  yield result
```

`.text` returns `DynamicLoopResult[String]`; `.asResponse` returns
`DynamicLoopResult[Output]`. Both expose:

- `output` and the final `stopReason`;
- `turns: List[LoopTurn]`, including each turn number, stop reason,
  `TokenUsage`, `Metrics`, and requested tool names;
- `totals`, containing aggregate input/output/total tokens and cache tokens.

Converse reports model latency in `Metrics.latencyMs`; Mantle's OpenAI Chat
Completions response currently does not, so Mantle latency is `null` rather than
an invented zero. Aggregate loop latency is also `null` if any turn is unknown.
Cache fields are populated whenever the selected backend supplies them.

```scala
result.turns.foreach: turn =>
  println(s"turn=${turn.turn} usage=${turn.usage} tools=${turn.toolNames}")

println(s"total usage: ${result.totals.usage}")
println(s"total model latency: ${result.totals.latencyMs} ms")
```

`dynamicLoop` owns the full assistant/tool-result history, returns results with
matching tool-call IDs, dispatches multiple calls in their emitted order, and
fails with `Bedrock.Error.MaxIterations` at the configured limit. Handler
failures propagate through the ZIO error channel. To let the model recover
from a tool failure instead, return a `DynamicToolResult` with
`status = ToolResultStatus.Error`; because Chat Completions has no native tool
status field, the library encodes that status into the tool message content.

## Single-turn with tools (`Bedrock.request`)

Same `tools` NamedTuple, but the framework runs the tool exactly once
and hands the typed output to a `.fold` whose keys mirror the tool
keys. Use this when you want the model's tool call to be the answer
(no follow-up turn).

```scala
Bedrock.request("generate a 16 character random string", tools).fold[Unit]:
  (
    randomLetters = s => println(s"tool result: $s"),
    reverse       = s => println("should not happen"),
  )
```

The `.fold` is exhaustive: every key in `tools` needs a function from
its tool's typed output to a unified result type. Tool failures
propagate through the ZIO error channel as a typed union of every
handler's `E`.

To let the model reply with text or structured JSON instead of
dispatching a tool, register a `ModelResponseTool` in the NamedTuple:

```scala
case class Forecast(city: String, summary: String) derives Schema

val tools = (
  randomLetters = ToolHandler(…),
  reverse       = ToolHandler.fromPure(…),
  reply         = ModelResponseTool[Forecast]("Summarise the answer."),
)

Bedrock.request("…", tools).fold[Forecast]:
  (
    randomLetters = s => Forecast("?", s),
    reverse       = s => Forecast("?", s),
    reply         = f => f,
  )
```

## Low-level tools (`Bedrock.chat`)

When you want full control — drive the round-trip yourself, decide on
the fly whether to send the result back, build custom message
sequences. This is the same wire shape `Bedrock.request` uses
internally.

```scala
import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.*
import zio.*
import zio.http.Client
import zio.schema.{Schema, derived}

case class WeatherInput(city: String) derives Schema
case class WeatherOutput(temperatureF: Int, conditions: String) derives Schema

def get_weather(in: WeatherInput): WeatherOutput =
  WeatherOutput(temperatureF = 64, conditions = "foggy")

object Weather extends ZIOAppDefault:
  def run =
    val tool: Tool[WeatherInput] =
      get_weather.asTool("Get the current weather (F + conditions) for a US city.")
    // tool.name == ToolName("get_weather")  (derived from the function reference)

    val initial = RequestConfig(
      messages   = List(Message.user("What's the weather in San Francisco?")),
      toolConfig = ToolConfig(tools = List(tool)),
    )

    val program = Bedrock.chat(initial).asResponse.flatMap: first =>
      val toolCall = first.output.message.content.collectFirst:
        case ContentBlock.ToolUse(id, name, input) => (id, name, input)

      toolCall match
        case Some((toolUseId, _, input)) =>
          val answer = input.as[WeatherInput].fold(
            err => WeatherOutput(0, s"input decode failed: $err"),
            get_weather,
          )
          val followup = initial.copy(
            messages = initial.messages
              :+ first.output.message
              :+ Message(Role.User, List(ContentBlock.ToolResult(
                toolUseId = toolUseId,
                content   = List(ToolResultBlock.json(answer)),
              ))),
          )
          Bedrock.chat(followup).text
        case None =>
          ZIO.succeed(first.output.text)

    program.debug("answer").provide(Mantle.live)
```

## Tests

The default suite includes real local ZIO HTTP protocol tests for both backends.
`MantleProtocolSpec` validates bearer auth, exact Chat Completions JSON,
structured output, two-turn function tools, usage, and SSE. `ConverseProtocolSpec`
validates native Converse JSON, model-path encoding, tool use/results,
usage/cache/latency, reasoning events, AWS EventStream framing, and errors.
Neither needs AWS credentials.

```bash
./sbt test
```

When `AWS_BEARER_TOKEN_BEDROCK` is available, both live suites run against
AWS. Backend-specific variables only override the model and region.

```bash
AWS_BEARER_TOKEN_BEDROCK=… \
BEDROCK_MANTLE_TEST_MODEL_ID=openai.gpt-oss-120b \
./sbt 'testOnly com.jamesward.zio_bedrock.MantleIntegrationSpec'

AWS_BEARER_TOKEN_BEDROCK=… \
BEDROCK_CONVERSE_TEST_MODEL_ID=us.anthropic.claude-sonnet-4-5-20250929-v1:0 \
./sbt 'testOnly com.jamesward.zio_bedrock.ConverseIntegrationSpec'
```
