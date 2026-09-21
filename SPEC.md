# zio-bedrock

A Scala 3 / ZIO library exposing one typed API over two Amazon Bedrock HTTP backends:

- native Converse and ConverseStream at `https://bedrock-runtime.<region>.amazonaws.com/model/<model-id>/converse` and `.../converse-stream`; and
- OpenAI-compatible Chat Completions at `https://bedrock-mantle.<region>.api.aws/v1/chat/completions`.

The published coordinates are `com.jamesward::zio-bedrock`. Public packages are rooted at `com.jamesward.zio_bedrock`; backend DTOs remain private.

## Public API

Both `Mantle` and `Converse` layers provide the shared `Bedrock` service. The same `ZIO[Bedrock, Bedrock.Error, A]` program can run unchanged through either backend.

- `Bedrock.chat` performs one low-level model turn and exposes text, structured output, tool calls, and streaming.
- `Bedrock.request` performs one turn and dispatches one typed handler into an exhaustive `NamedTuple` fold.
- `Bedrock.loop` owns a bounded typed tool loop.
- `Bedrock.dynamicLoop` owns a bounded loop over runtime-provided JSON Schema tool definitions and returns per-turn and aggregate metrics.

`ModelId`, `ApiKey`, `ToolName`, and `ToolUseId` are opaque string types. `ToolInput` hides `DynamicValue`; callers decode it with `as[I: Schema]` or inspect it with `asJsonObject`.

## Configuration and layers

Both backends authenticate with `Authorization: Bearer <Bedrock API key>`; SigV4 is not implemented.

`Mantle.configured` and `Converse.configured` both require `AWS_BEARER_TOKEN_BEDROCK` and `BEDROCK_MODEL_ID`, and default a missing `AWS_REGION` to `us-east-1`. Either layer fails with a typed `InvalidRegion` when a non-empty unsupported region is supplied.

`Mantle.layer(config)` and `Converse.layer(config)` require an existing `zio.http.Client`. `Mantle.live` and `Converse.live` are self-contained and include `Client.default`. Passing a custom origin keeps each backend's standard request paths. Configuration string rendering must redact API keys.

## Normalized protocol

Shared builders produce a private normalized `Wire.ChatRequest`. `MantleWire` translates it to OpenAI Chat Completions JSON; `ConverseWire` translates it to native Bedrock JSON.

Common behavior:

- system messages, user/assistant turns, tool use/results, inference options, structured output, stop reasons, usage, cache counts, and metrics map through the normalized model;
- typed tools derive input/output/error schemas from `zio.schema.Schema`;
- dynamic tools retain supplied `zio.json.ast.Json.Obj` schemas verbatim;
- primitive tool inputs are wrapped as an object on the wire and unwrapped before handler dispatch;
- same-turn handlers execute concurrently with maximum parallelism 8, while result order follows tool-use order;
- typed-loop handler failures are encoded as tool errors for model recovery; dynamic handler failures remain in the caller's error channel unless represented by `DynamicToolResult` with error status; and
- all loops are bounded by `maxIterations`.


Public `Message` values returned by a backend retain their exact normalized
`WireMessage` privately. Reusing such a message in `RequestConfig` therefore
preserves provider continuation blocks, including signed Converse reasoning,
without exposing them in public content, equality, or rendering. `Message.portable`
explicitly discards the retained form. Managed loops always continue from the
exact normalized response.

Mantle maps normalized history to OpenAI `assistant.tool_calls` and `role=tool` messages. Because Chat Completions has no native tool-result status, error status is encoded into tool message content. Converse preserves native content blocks, including reasoning signatures needed for continuation.

## Structured output

Structured terminals generate an inline JSON Schema with `additionalProperties=false` recursively on object schemas. Successful text is decoded with the requested `Schema[T]`; failures surface as `Bedrock.Error.StructuredDecode`.

## Metrics

`Metrics.latencyMs` is `Long | Null`. Converse preserves reported latency. Mantle reports `null` because Chat Completions currently provides no model-latency field. Aggregate loop latency is `null` if any constituent turn has unknown latency. Optional cache token counts remain `null` when not reported.

## Streaming

Mantle consumes SSE and accepts either `[DONE]` or a clean stream end after a `finish_reason` chunk; ending before `finish_reason` is treated as truncation. Converse consumes binary AWS EventStream frames and validates required headers, prelude CRC32, message CRC32, and complete framing.

`Bedrock.chat(...).asStream` exposes normalized deltas, block stops, message stop, metadata, and exactly one final `StreamEvent.Complete`. The completion contains the public `Result[Output]` and privately retains the exact normalized backend response. A stream that is truncated, emits duplicate completion, or emits data after completion fails with `Bedrock.Error` rather than ending silently.

Streaming loops use the retained normalized response for continuation. Intermediate tool turns do not expose `Complete`; loop `asStream` emits one terminal completion for the final turn. Loop `textStream` suppresses text from intermediate tool-dispatch turns and emits only final-reply text chunks.

## Errors and retry

HTTP failures map to the shared hierarchy (`Validation`, `AccessDenied`, `ResourceNotFound`, `ModelTimeout`, `ModelErr`, `Throttling`, `InternalServer`, and `ServiceUnavailable`). Transport, protocol decode, structured-output decode, unknown tool, invalid input, unexpected reply, and maximum-iteration failures remain explicit.

`retryOnRetryable` retries throttling, timeout, internal-server, and service-unavailable errors with exponential backoff for up to two retries.

## Tests

Deterministic default tests include:

- `BedrockMockSpec` for shared builders, typed request/loop behavior, and stream completion semantics;
- `DynamicToolSpec` for runtime schemas, history, ordering, concurrency, and aggregate metrics;
- `MantleProtocolSpec` for exact Chat Completions transport, auth, tools, structured output, SSE completion, and truncated-stream rejection;
- `ConverseProtocolSpec` for native paths/JSON, auth, tools, signed reasoning continuation, binary AWS EventStream decoding/checksums/errors, and terminal completion;
- `MantleConfigSpec` and `ConverseConfigSpec` for environment loading and key redaction; and
- `DualBackendSpec` proving one explicitly typed shared program runs through both local backend layers.

Live suites are `MantleIntegrationSpec` and `ConverseIntegrationSpec`. Both are gated by `AWS_BEARER_TOKEN_BEDROCK`; backend-specific `BEDROCK_MANTLE_TEST_*` and `BEDROCK_CONVERSE_TEST_*` variables only override model and region. When the shared bearer token is present, ordinary `sbt test` includes both live suites.
