package com.jamesward.zio_bedrock.internal

import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.*
import com.jamesward.zio_bedrock.internal.Codecs.given
import zio.json.*
import zio.json.ast.Json
import zio.schema.annotation.fieldName
import zio.schema.codec.json.schemaJson
import zio.schema.{DynamicValue, Schema, derived}

/** Exact OpenAI Chat Completions shapes used by the Bedrock Mantle endpoint.
  * The rest of the library works with a normalized conversation model; this
  * object is the only protocol translation boundary.
  */
private[zio_bedrock] object MantleWire:

  case class FunctionCall(name: String, arguments: String) derives Schema

  case class ToolCall(
    id: String,
    `type`: String = "function",
    function: FunctionCall,
  ) derives Schema

  case class Message(
    role: String,
    content: Option[String] = None,
    name: Option[String] = None,
    @fieldName("tool_calls") toolCalls: List[ToolCall] = Nil,
    @fieldName("tool_call_id") toolCallId: Option[String] = None,
  ) derives Schema

  case class FunctionDefinition(
    name: String,
    description: Option[String] = None,
    parameters: Json,
    strict: Option[Boolean] = None,
  ) derives Schema

  case class Tool(`type`: String = "function", function: FunctionDefinition) derives Schema

  case class JsonSchemaDefinition(
    name: String,
    description: Option[String] = None,
    schema: Json,
    strict: Option[Boolean] = Some(true),
  ) derives Schema

  case class ResponseFormat(
    `type`: String = "json_schema",
    @fieldName("json_schema") jsonSchema: JsonSchemaDefinition,
  ) derives Schema

  case class StreamOptions(@fieldName("include_usage") includeUsage: Boolean = true) derives Schema

  case class Request(
    model: String,
    messages: List[Message],
    temperature: Option[Double] = None,
    @fieldName("top_p") topP: Option[Double] = None,
    @fieldName("max_tokens") maxTokens: Option[Int] = None,
    stop: List[String] = Nil,
    tools: List[Tool] = Nil,
    @fieldName("tool_choice") toolChoice: Option[Json] = None,
    @fieldName("response_format") responseFormat: Option[ResponseFormat] = None,
    stream: Option[Boolean] = None,
    @fieldName("stream_options") streamOptions: Option[StreamOptions] = None,
  ) derives Schema

  case class PromptTokensDetails(@fieldName("cached_tokens") cachedTokens: Option[Int] = None) derives Schema

  case class Usage(
    @fieldName("prompt_tokens") promptTokens: Int = 0,
    @fieldName("completion_tokens") completionTokens: Int = 0,
    @fieldName("total_tokens") totalTokens: Int = 0,
    @fieldName("prompt_tokens_details") promptTokensDetails: Option[PromptTokensDetails] = None,
  ) derives Schema

  case class Choice(
    index: Int,
    message: Message,
    @fieldName("finish_reason") finishReason: Option[String] = None,
  ) derives Schema

  case class Response(
    id: String,
    `object`: String,
    created: Long,
    model: String,
    choices: List[Choice],
    usage: Option[Usage] = None,
  ) derives Schema

  case class DeltaFunction(
    name: Option[String] = None,
    arguments: Option[String] = None,
  ) derives Schema

  case class DeltaToolCall(
    index: Int,
    id: Option[String] = None,
    `type`: Option[String] = None,
    function: Option[DeltaFunction] = None,
  ) derives Schema

  case class Delta(
    role: Option[String] = None,
    content: Option[String] = None,
    @fieldName("tool_calls") toolCalls: List[DeltaToolCall] = Nil,
  ) derives Schema

  case class ChunkChoice(
    index: Int,
    delta: Delta,
    @fieldName("finish_reason") finishReason: Option[String] = None,
  ) derives Schema

  case class Chunk(
    id: String,
    `object`: String,
    created: Long,
    model: String,
    choices: List[ChunkChoice] = Nil,
    usage: Option[Usage] = None,
  ) derives Schema

  def request(normalized: Wire.ChatRequest, modelId: ModelId, streaming: Boolean): Request =
    val systemMessages = normalized.system.collect:
      case Wire.SystemContentBlock.Text(text) => Message("system", Some(text))
      case _                                  => Message("system", None)

    val messages = systemMessages ++ normalized.messages.flatMap(toMessages)
    val inference = normalized.inferenceConfig
    val tools = normalized.toolConfig.toList.flatMap(_.tools).collect:
      case Wire.ToolDef.ToolSpec(spec) =>
        Tool(function = FunctionDefinition(
          name = spec.name.unwrap,
          description = spec.description,
          parameters = spec.jsonSchema,
          strict = spec.strict,
        ))
    val toolChoice = normalized.toolConfig.flatMap(_.toolChoice).map:
      case Wire.ToolChoice.Auto(_) => Json.Str("auto")
      case Wire.ToolChoice.Any(_)  => Json.Str("required")
      case Wire.ToolChoice.Tool(tool) => Json.Obj(
        "type" -> Json.Str("function"),
        "function" -> Json.Obj("name" -> Json.Str(tool.name.unwrap)),
      )
    val responseFormat = normalized.outputConfig.map: output =>
      val spec = output.textFormat match
        case Wire.TextFormat.JsonSchema(structure) => structure.jsonSchema
      val schema = spec.schema.fromJson[Json].getOrElse(Json.Obj())
      ResponseFormat(jsonSchema = JsonSchemaDefinition(
        name = spec.name,
        description = spec.description,
        schema = schema,
      ))

    Request(
      model = modelId.unwrap,
      messages = messages,
      temperature = inference.flatMap(i => optional(i.temperature)),
      topP = inference.flatMap(i => optional(i.topP)),
      maxTokens = inference.flatMap(i => optional(i.maxTokens)),
      stop = inference.toList.flatMap(_.stopSequences),
      tools = tools,
      toolChoice = toolChoice,
      responseFormat = responseFormat,
      stream = if streaming then Some(true) else None,
      streamOptions = if streaming then Some(StreamOptions()) else None,
    )

  def response(value: Response): Either[String, Wire.ChatResponse] =
    value.choices.headOption.toRight("Mantle response contained no choices").flatMap: choice =>
      val content = choice.message.content.filter(_.nonEmpty).toList.map(Wire.ContentBlock.Text.apply)
      val toolUses = choice.message.toolCalls.map: call =>
        for
          arguments <- call.function.arguments.fromJson[Json]
        yield Wire.ContentBlock.ToolUse(Wire.ToolUseContent(
          ToolUseId(call.id),
          ToolName(call.function.name),
          summon[Schema[Json]].toDynamic(arguments),
        ))
      sequence(toolUses).map: calls =>
        val usage = toUsage(value.usage)
        Wire.ChatResponse(
          Wire.ChatOutput(Wire.WireMessage(Role.Assistant, content ++ calls)),
          toStopReason(choice.finishReason),
          usage,
          Metrics(),
        )

  private[zio_bedrock] final class StreamAccumulator:
    private val text = new StringBuilder
    private val toolCalls = scala.collection.mutable.Map.empty[Int, (String, String, StringBuilder)]
    private var stopReason: StopReason = StopReason.EndTurn
    private var sawFinishReason: Boolean = false
    private var usage: TokenUsage = TokenUsage(0, 0, 0)

    def add(chunk: Chunk): List[Bedrock.StreamEvent] =
      chunk.choices.foreach: choice =>
        choice.delta.content.foreach(text.append)
        choice.delta.toolCalls.foreach: call =>
          val previous = toolCalls.get(call.index)
          val id = call.id.orElse(previous.map(_._1)).getOrElse("")
          val name = call.function.flatMap(_.name).orElse(previous.map(_._2)).getOrElse("")
          val args = previous.map(_._3).getOrElse(new StringBuilder)
          call.function.flatMap(_.arguments).foreach(args.append)
          toolCalls(call.index) = (id, name, args)
        choice.finishReason.foreach: reason =>
          stopReason = toStopReason(Some(reason))
          sawFinishReason = true
      chunk.usage.foreach(value => usage = toUsage(Some(value)))
      streamEvents(chunk)

    def complete: Either[String, Bedrock.StreamEvent] =
      if !sawFinishReason then Left("stream ended before finish_reason")
      else
        val tools = toolCalls.toList.sortBy(_._1).map: (_, value) =>
          val (id, name, input) = value
          input.toString.fromJson[Json].map: json =>
            Wire.ContentBlock.ToolUse(Wire.ToolUseContent(
              ToolUseId(id), ToolName(name), summon[Schema[Json]].toDynamic(json),
            ))
        sequence(tools).map: toolContent =>
          val content = Option.when(text.nonEmpty)(Wire.ContentBlock.Text(text.toString)).toList ++ toolContent
          val response = Wire.ChatResponse(
            Wire.ChatOutput(Wire.WireMessage(Role.Assistant, content)),
            stopReason,
            usage,
            Metrics(),
          )
          Bedrock.StreamEvent.Complete(Bedrock.StreamComplete.fromWire(response))

  def streamEvents(chunk: Chunk): List[Bedrock.StreamEvent] =
    val choiceEvents = chunk.choices.flatMap: choice =>
      val content = choice.delta.content.toList.filter(_.nonEmpty).map(Bedrock.StreamEvent.TextDelta.apply)
      val tools = choice.delta.toolCalls.flatMap: call =>
        val start = (call.id, call.function.flatMap(_.name)) match
          case (Some(id), Some(name)) => List(Bedrock.StreamEvent.ToolUseStart(ToolUseId(id), ToolName(name)))
          case _                      => Nil
        val delta = call.function.flatMap(_.arguments).filter(_.nonEmpty).toList.map: args =>
          Bedrock.StreamEvent.ToolUseDelta(call.id.map(ToolUseId(_)).getOrElse(ToolUseId("")), args)
        start ++ delta
      val finish = choice.finishReason.toList.flatMap:
        case "tool_calls" => List(
          Bedrock.StreamEvent.ContentBlockStop(choice.index),
          Bedrock.StreamEvent.MessageStop(StopReason.ToolUse),
        )
        case reason => List(Bedrock.StreamEvent.MessageStop(toStopReason(Some(reason))))
      content ++ tools ++ finish
    val metadata = chunk.usage.toList.map: usage =>
      Bedrock.StreamEvent.Metadata(toUsage(Some(usage)), Metrics())
    choiceEvents ++ metadata

  private def toMessages(message: Wire.WireMessage): List[Message] =
    val texts = message.content.collect { case Wire.ContentBlock.Text(text) => text }
    val toolUses = message.content.collect:
      case Wire.ContentBlock.ToolUse(value) => ToolCall(
        id = value.toolUseId.unwrap,
        function = FunctionCall(value.name.unwrap, dynamicJson(value.input).toJson),
      )
    val toolResults = message.content.collect:
      case Wire.ContentBlock.ToolResult(value) => Message(
        role = "tool",
        content = Some(toolResultContent(value)),
        toolCallId = Some(value.toolUseId.unwrap),
      )

    message.role match
      case Role.Assistant =>
        if texts.nonEmpty || toolUses.nonEmpty then
          List(Message("assistant", nonEmpty(texts.mkString), toolCalls = toolUses))
        else Nil
      case Role.User =>
        val user = if texts.nonEmpty then List(Message("user", Some(texts.mkString))) else Nil
        user ++ toolResults

  private def toolResultContent(result: Wire.ToolResultContent): String =
    val content = result.content.map:
      case Wire.ToolResultBlock.Text(text) => text
      case Wire.ToolResultBlock.Json(json) => dynamicJson(json).toJson
      case other                           => other.toString
    .mkString("\n")
    result.status match
      case Some(Wire.ToolResultStatus.Error) =>
        Json.Obj("status" -> Json.Str("error"), "content" -> Json.Str(content)).toJson
      case _ => content

  private def dynamicJson(value: DynamicValue): Json =
    summon[Schema[Json]].fromDynamic(value).getOrElse(Json.Obj())

  private def toUsage(value: Option[Usage]): TokenUsage =
    val usage = value.getOrElse(Usage())
    TokenUsage(
      usage.promptTokens,
      usage.completionTokens,
      usage.totalTokens,
      usage.promptTokensDetails.flatMap(_.cachedTokens).map(_.asInstanceOf[Int | Null]).getOrElse(null),
      null,
    )

  private def toStopReason(value: Option[String]): StopReason = value match
    case Some("tool_calls")     => StopReason.ToolUse
    case Some("length")         => StopReason.MaxTokens
    case Some("content_filter") => StopReason.ContentFiltered
    case _                      => StopReason.EndTurn

  private def optional[A](value: A | Null): Option[A] =
    if value.asInstanceOf[AnyRef] eq null then None else Some(value.asInstanceOf[A])

  private def nonEmpty(value: String): Option[String] = Option.when(value.nonEmpty)(value)

  private def sequence[A](values: List[Either[String, A]]): Either[String, List[A]] =
    values.foldRight[Either[String, List[A]]](Right(Nil))((next, acc) => next.flatMap(v => acc.map(v :: _)))
