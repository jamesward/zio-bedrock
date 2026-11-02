package com.jamesward.zio_bedrock.internal

import com.jamesward.zio_bedrock.Bedrock.*
import zio.schema.{Schema, derived}
import zio.schema.annotation.caseName
import zio.json.ast.Json
import zio.schema.codec.json.schemaJson

/** Exact JSON envelope used by Amazon Bedrock's native Converse API.
  * Shared builders produce [[Wire.ChatRequest]]; this boundary supplies the
  * backend-specific stop-reason names without exposing another public model.
  */
private[zio_bedrock] object ConverseWire:

  case class Request(
    messages:        List[Wire.WireMessage],
    system:          List[Wire.SystemContentBlock]   = Nil,
    inferenceConfig: Option[Inference]               = None,
    toolConfig:      Option[Wire.ToolConfig]         = None,
    outputConfig:    Option[Wire.OutputConfig]       = None,
    requestMetadata: Map[String, String]             = Map.empty,
    additionalModelResponseFieldPaths: List[String] = Nil,
    additionalModelRequestFields: Option[Json] = None,
  ) derives Schema

  /** Converse's `inferenceConfig`: the public one without what Converse takes elsewhere (the reasoning effort). */
  case class Inference(
    maxTokens:     Option[Int]    = None,
    temperature:   Option[Double] = None,
    topP:          Option[Double] = None,
    stopSequences: List[String]   = Nil,
  ) derives Schema

  private def optional[A](value: A | Null): Option[A] = Option(value.asInstanceOf[AnyRef]).map(_.asInstanceOf[A])

  enum StopReason derives Schema:
    @caseName("end_turn")                      case EndTurn
    @caseName("tool_use")                      case ToolUse
    @caseName("max_tokens")                    case MaxTokens
    @caseName("stop_sequence")                 case StopSequence
    @caseName("guardrail_intervened")          case GuardrailIntervened
    @caseName("content_filtered")              case ContentFiltered
    @caseName("malformed_model_output")        case MalformedModelOutput
    @caseName("malformed_tool_use")            case MalformedToolUse
    @caseName("model_context_window_exceeded") case ModelContextWindowExceeded
  given CanEqual[StopReason, StopReason] = CanEqual.derived

  case class Response(
    output:     Wire.ChatOutput,
    stopReason: StopReason,
    usage:      TokenUsage,
    metrics:    Metrics,
  ) derives Schema

  def request(value: Wire.ChatRequest): Request =
    Request(
      messages = value.messages,
      system = value.system,
      inferenceConfig = value.inferenceConfig.map(i =>
        Inference(optional(i.maxTokens), optional(i.temperature), optional(i.topP), i.stopSequences)),
      toolConfig = value.toolConfig,
      outputConfig = value.outputConfig,
      requestMetadata = value.requestMetadata,
      additionalModelResponseFieldPaths = value.additionalModelResponseFieldPaths,
      additionalModelRequestFields = value.inferenceConfig.flatMap(i => optional(i.reasoningEffort))
        .map(effort => Json.Obj("reasoning_effort" -> Json.Str(effort))),
    )

  def response(value: Response): Wire.ChatResponse =
    Wire.ChatResponse(
      output = value.output,
      stopReason = value.stopReason match
        case StopReason.EndTurn                    => com.jamesward.zio_bedrock.Bedrock.StopReason.EndTurn
        case StopReason.ToolUse                    => com.jamesward.zio_bedrock.Bedrock.StopReason.ToolUse
        case StopReason.MaxTokens                  => com.jamesward.zio_bedrock.Bedrock.StopReason.MaxTokens
        case StopReason.StopSequence               => com.jamesward.zio_bedrock.Bedrock.StopReason.StopSequence
        case StopReason.GuardrailIntervened        => com.jamesward.zio_bedrock.Bedrock.StopReason.GuardrailIntervened
        case StopReason.ContentFiltered            => com.jamesward.zio_bedrock.Bedrock.StopReason.ContentFiltered
        case StopReason.MalformedModelOutput       => com.jamesward.zio_bedrock.Bedrock.StopReason.MalformedModelOutput
        case StopReason.MalformedToolUse           => com.jamesward.zio_bedrock.Bedrock.StopReason.MalformedToolUse
        case StopReason.ModelContextWindowExceeded => com.jamesward.zio_bedrock.Bedrock.StopReason.ModelContextWindowExceeded,
      usage = value.usage,
      metrics = value.metrics,
    )
