package com.jamesward.zio_bedrock.internal

import com.jamesward.zio_bedrock.{Bedrock, ConverseConfig}
import com.jamesward.zio_bedrock.Bedrock.*
import com.jamesward.zio_bedrock.internal.Codecs.given
import zio.*
import zio.http.*
import zio.json.*
import zio.schema.{Schema}
import zio.schema.codec.json.schemaJson
import zio.schema.codec.JsonCodec
import zio.stream.*

/** Native Bedrock Runtime transport for Converse and ConverseStream. */
private[zio_bedrock] object ConverseTransport:

  def build(config: ConverseConfig, client: Client): Bedrock =
    val endpoint = Option(config.endpoint.asInstanceOf[URL]).getOrElse:
      URL.decode(s"https://bedrock-runtime.${config.region.code}.amazonaws.com").toOption.get
    val authedClient = client
      .url(endpoint)
      .addHeader(Header.Authorization.Bearer(config.apiKey.unwrap))
      .addHeader(Header.ContentType(MediaType.application.json))
    val transport = new ConverseProtocol(config.modelId, authedClient)
    new Bedrock:
      private[zio_bedrock] val protocol: Protocol = transport

  private final class ConverseProtocol(
    modelId: Bedrock.ModelId,
    hc: Client,
  ) extends Protocol:

    private val requestCodec = JsonCodec.schemaBasedBinaryCodec[ConverseWire.Request](Codecs.codecConfig)
    private val responseCodec = JsonCodec.schemaBasedBinaryCodec[ConverseWire.Response](Codecs.codecConfig)
    private val path = s"/model/${pathEncode(modelId.unwrap)}/converse"
    private val streamPath = s"/model/${pathEncode(modelId.unwrap)}/converse-stream"

    def send(req: Wire.ChatRequest): IO[Error, Wire.ChatResponse] =
      val body = Body.fromChunk(requestCodec.encode(ConverseWire.request(req)))
      ZIO.scoped:
        hc.post(path)(body)
          .mapError(Error.Transport.apply)
          .flatMap: response =>
            if response.status.isSuccess then
              response.body.asChunk
                .mapError(Error.Transport.apply)
                .flatMap: bytes =>
                  ZIO.fromEither(responseCodec.decode(bytes))
                    .mapError(error => Error.Unexpected(response.status, s"Decode failed: ${error.message}"))
                    .map(ConverseWire.response)
            else failResponse(response)

    def sendStreamEvents(req: Wire.ChatRequest): ZStream[Any, Error, Bedrock.StreamEvent] =
      val body = Body.fromChunk(requestCodec.encode(ConverseWire.request(req)))
      ZStream.unwrapScoped:
        hc.post(streamPath)(body)
          .mapError(Error.Transport.apply)
          .flatMap: response =>
            if response.status.isSuccess then
              ZIO.succeed:
                val accumulator = new StreamAccumulator
                response.body.asStream
                  .mapError(Error.Transport.apply)
                  .via(AwsEventStream.parseFrames)
                  .mapZIO(event => ZIO.fromEither(accumulator.add(event)))
                  .mapConcat(identity)
            else failResponse(response)

    private def failResponse(response: Response): IO[Error, Nothing] =
      response.body.asString
        .mapError(Error.Transport.apply)
        .flatMap(text => ZIO.fail(Error.fromStatus(response.status, text)))

  /** Model IDs flow into the URL path literally. AWS rejects over-encoded
    * paths (`%3A` instead of `:`); only spaces are escaped defensively.
    */
  private def pathEncode(value: String): String = value.replace(" ", "%20")

  private sealed trait AccumulatedBlock
  private final case class TextBlock(text: StringBuilder) extends AccumulatedBlock
  private final case class ReasoningBlock(
    text: StringBuilder,
    var signature: Option[String],
    var redactedContent: Option[String],
  ) extends AccumulatedBlock
  private final case class ToolBlock(
    id: ToolUseId,
    name: ToolName,
    input: StringBuilder,
  ) extends AccumulatedBlock

  /** Accumulates the native stream into the same normalized response returned
    * by Converse. The retained reasoning/signature blocks are used verbatim by
    * shared loops when constructing the next turn. */
  private final class StreamAccumulator:
    private val blocks = scala.collection.mutable.Map.empty[Int, AccumulatedBlock]
    private var stopReason: StopReason = StopReason.EndTurn

    def add(event: AwsEventStream.Event): Either[Error, List[Bedrock.StreamEvent]] =
      import Bedrock.StreamEvent.*
      if event.messageType == "exception" then Left(streamError(event.eventType, event.payload))
      else if event.messageType == "error" then
        Left(Error.Unexpected(Status.InternalServerError, s"${event.eventType}: ${event.payload}"))
      else event.eventType match
        case "contentBlockStart" =>
          val index = extractJsonInt(event.payload, "contentBlockIndex").getOrElse(0)
          (extractJsonString(event.payload, "toolUseId"), extractJsonString(event.payload, "name")) match
            case (Some(id), Some(name)) =>
              val tool = ToolBlock(ToolUseId(id), ToolName(name), new StringBuilder)
              blocks(index) = tool
              Right(List(ToolUseStart(tool.id, tool.name)))
            case _ => Right(Nil)
        case "contentBlockDelta" =>
          val index = extractJsonInt(event.payload, "contentBlockIndex").getOrElse(0)
          if event.payload.contains("\"reasoningContent\"") then
            val block = blocks.get(index) match
              case Some(value: ReasoningBlock) => value
              case _ =>
                val value = ReasoningBlock(new StringBuilder, None, None)
                blocks(index) = value
                value
            val text = extractJsonString(event.payload, "text")
            text.foreach(block.text.append)
            extractJsonString(event.payload, "signature").foreach(value => block.signature = Some(value))
            extractJsonString(event.payload, "redactedContent").foreach(value => block.redactedContent = Some(value))
            Right(text.map(value => List(ReasoningDelta(value))).getOrElse(Nil))
          else if event.payload.contains("\"toolUse\"") then
            blocks.get(index) match
              case Some(block: ToolBlock) =>
                val input = extractJsonString(event.payload, "input").getOrElse("")
                block.input.append(input)
                Right(Option.when(input.nonEmpty)(ToolUseDelta(block.id, input)).toList)
              case _ => Right(Nil)
          else
            val delta = extractTextDelta(event.payload).getOrElse("")
            val block = blocks.get(index) match
              case Some(value: TextBlock) => value
              case _ =>
                val value = TextBlock(new StringBuilder)
                blocks(index) = value
                value
            block.text.append(delta)
            Right(Option.when(delta.nonEmpty)(TextDelta(delta)).toList)
        case "contentBlockStop" =>
          Right(List(ContentBlockStop(extractJsonInt(event.payload, "contentBlockIndex").getOrElse(0))))
        case "messageStop" =>
          stopReason = extractJsonString(event.payload, "stopReason").map(streamStopReason).getOrElse(StopReason.EndTurn)
          Right(List(MessageStop(stopReason)))
        case "metadata" =>
          val usage = Bedrock.TokenUsage(
            extractJsonInt(event.payload, "inputTokens").getOrElse(0),
            extractJsonInt(event.payload, "outputTokens").getOrElse(0),
            extractJsonInt(event.payload, "totalTokens").getOrElse(0),
            extractJsonInt(event.payload, "cacheReadInputTokens").map(_.asInstanceOf[Int | Null]).getOrElse(null),
            extractJsonInt(event.payload, "cacheWriteInputTokens").map(_.asInstanceOf[Int | Null]).getOrElse(null),
          )
          val metrics = Bedrock.Metrics(
            extractJsonLong(event.payload, "latencyMs").getOrElse(0L),
          )
          complete(usage, metrics).map(response =>
            List(Metadata(usage, metrics), Complete(Bedrock.StreamComplete.fromWire(response))),
          )
        case streamException if streamException.endsWith("Exception") =>
          Left(streamError(streamException, event.payload))
        case _ => Right(Nil)

    private def complete(usage: Bedrock.TokenUsage, metrics: Bedrock.Metrics): Either[Error, Wire.ChatResponse] =
      val content = blocks.toList.sortBy(_._1).map(_._2).map:
        case TextBlock(text) => Right(Wire.ContentBlock.Text(text.toString))
        case ReasoningBlock(text, signature, redactedContent) =>
          Right(Wire.ContentBlock.ReasoningContent(Wire.ReasoningContentBlock(
            reasoningText = Option.when(text.nonEmpty || signature.nonEmpty)(Wire.ReasoningTextBlock(text.toString, signature)),
            redactedContent = redactedContent,
          )))
        case ToolBlock(id, name, input) =>
          input.toString.fromJson[zio.json.ast.Json]
            .left.map(message => Error.Unexpected(Status.InternalServerError, s"Tool input decode failed: $message"))
            .map(json => Wire.ContentBlock.ToolUse(Wire.ToolUseContent(
              id, name, summon[Schema[zio.json.ast.Json]].toDynamic(json),
            )))
      sequence(content).map(blocks => Wire.ChatResponse(
        Wire.ChatOutput(Wire.WireMessage(Bedrock.Role.Assistant, blocks)),
        stopReason,
        usage,
        metrics,
      ))

  private def sequence[A](values: List[Either[Error, A]]): Either[Error, List[A]] =
    values.foldRight[Either[Error, List[A]]](Right(Nil))((next, rest) => next.flatMap(value => rest.map(value :: _)))

  private def streamStopReason(value: String): StopReason = value match
    case "tool_use"                     => StopReason.ToolUse
    case "max_tokens"                   => StopReason.MaxTokens
    case "stop_sequence"                => StopReason.StopSequence
    case "guardrail_intervened"         => StopReason.GuardrailIntervened
    case "content_filtered"             => StopReason.ContentFiltered
    case "malformed_model_output"       => StopReason.MalformedModelOutput
    case "malformed_tool_use"           => StopReason.MalformedToolUse
    case "model_context_window_exceeded" => StopReason.ModelContextWindowExceeded
    case _                              => StopReason.EndTurn

  private def streamError(kind: String, payload: String): Error =
    val message = extractJsonString(payload, "message").getOrElse(payload)
    kind match
      case "validationException"         => Error.Validation(message)
      case "throttlingException"         => Error.Throttling(message)
      case "modelTimeoutException"       => Error.ModelTimeout(message)
      case "modelStreamErrorException"   => Error.ModelErr(message, extractJsonInt(payload, "originalStatusCode").map(_.asInstanceOf[Int | Null]).getOrElse(null))
      case "serviceUnavailableException" => Error.ServiceUnavailable(message)
      case "internalServerException"     => Error.InternalServer(message)
      case _                              => Error.Unexpected(Status.InternalServerError, payload)

  private def extractJsonString(json: String, key: String): Option[String] =
    val marker = s"\"$key\":"
    val markerIndex = json.indexOf(marker)
    if markerIndex < 0 then None
    else
      var i = markerIndex + marker.length
      while i < json.length && json.charAt(i).isWhitespace do i += 1
      if i >= json.length || json.charAt(i) != '"' then None
      else
        i += 1
        val value = new StringBuilder
        var escaped = false
        while i < json.length do
          val char = json.charAt(i)
          if escaped then
            char match
              case '"'  => value.append('"')
              case '\\' => value.append('\\')
              case '/'  => value.append('/')
              case 'b'  => value.append('\b')
              case 'f'  => value.append('\f')
              case 'n'  => value.append('\n')
              case 'r'  => value.append('\r')
              case 't'  => value.append('\t')
              case other => value.append(other)
            escaped = false
          else if char == '\\' then escaped = true
          else if char == '"' then return Some(value.toString)
          else value.append(char)
          i += 1
        None

  private def extractJsonInt(json: String, key: String): Option[Int] =
    extractJsonLong(json, key).filter(value => value >= Int.MinValue && value <= Int.MaxValue).map(_.toInt)

  private def extractJsonLong(json: String, key: String): Option[Long] =
    val marker = s"\"$key\":"
    val markerIndex = json.indexOf(marker)
    if markerIndex < 0 then None
    else
      var i = markerIndex + marker.length
      while i < json.length && json.charAt(i).isWhitespace do i += 1
      val start = i
      if i < json.length && json.charAt(i) == '-' then i += 1
      while i < json.length && json.charAt(i).isDigit do i += 1
      if i == start || (i == start + 1 && json.charAt(start) == '-') then None
      else scala.util.Try(json.substring(start, i).toLong).toOption

  private def extractTextDelta(json: String): Option[String] =
    val deltaIndex = json.indexOf("\"delta\"")
    if deltaIndex < 0 || json.indexOf("\"reasoningContent\"", deltaIndex) >= 0 || json.indexOf("\"toolUse\"", deltaIndex) >= 0 then None
    else extractJsonString(json.substring(deltaIndex), "text")

  /** AWS Event Stream binary framing parser specialized for ConverseStream. */
  private[zio_bedrock] object AwsEventStream:

    final case class Event(messageType: String, eventType: String, payload: String)

    val parseFrames: ZPipeline[Any, Error, Byte, Event] =
      ZPipeline.fromPush:
        ZIO.succeed:
          var buffer = Chunk.empty[Byte]
          (incoming: Option[Chunk[Byte]]) =>
            incoming match
              case Some(chunk) =>
                buffer = buffer ++ chunk
                ZIO.fromEither(parseAll(buffer)).map: (events, remaining) =>
                  buffer = remaining
                  events
              case None =>
                if buffer.isEmpty then ZIO.succeed(Chunk.empty)
                else ZIO.fail(protocolError(s"truncated AWS EventStream frame (${buffer.length} trailing bytes)"))

    private def parseAll(data: Chunk[Byte]): Either[Error, (Chunk[Event], Chunk[Byte])] =
      val events = Chunk.newBuilder[Event]
      val bytes = data.toArray
      var position = 0
      while position + 12 <= bytes.length do
        val totalLength = readInt(bytes, position)
        val headersLength = readInt(bytes, position + 4)
        if totalLength < 16 then return Left(protocolError(s"invalid AWS EventStream frame length: $totalLength"))
        if headersLength < 0 || headersLength > totalLength - 16 then
          return Left(protocolError(s"invalid AWS EventStream headers length: $headersLength"))
        val expectedPreludeCrc = unsigned(readInt(bytes, position + 8))
        val actualPreludeCrc = crc32(bytes, position, 8)
        if expectedPreludeCrc != actualPreludeCrc then
          return Left(protocolError("invalid AWS EventStream prelude checksum"))
        if position + totalLength > bytes.length then
          return Right((events.result(), Chunk.fromArray(bytes.drop(position))))

        val expectedMessageCrc = unsigned(readInt(bytes, position + totalLength - 4))
        val actualMessageCrc = crc32(bytes, position, totalLength - 4)
        if expectedMessageCrc != actualMessageCrc then
          return Left(protocolError("invalid AWS EventStream message checksum"))

        val headersStart = position + 12
        val headersEnd = headersStart + headersLength
        val payloadEnd = position + totalLength - 4
        parseHeaders(bytes, headersStart, headersEnd) match
          case Left(error) => return Left(error)
          case Right(headers) =>
            val messageType = headers.get(":message-type")
              .toRight(protocolError("AWS EventStream frame is missing :message-type"))
            val eventType = messageType.flatMap:
              case "event" => headers.get(":event-type")
                .toRight(protocolError("AWS EventStream event is missing :event-type"))
              case "exception" => headers.get(":exception-type")
                .toRight(protocolError("AWS EventStream exception is missing :exception-type"))
              case "error" => headers.get(":error-code")
                .toRight(protocolError("AWS EventStream error is missing :error-code"))
              case other => Left(protocolError(s"unsupported AWS EventStream message type: $other"))
            (messageType, eventType) match
              case (Right(kind), Right(name)) =>
                val rawPayload = new String(bytes, headersEnd, payloadEnd - headersEnd, java.nio.charset.StandardCharsets.UTF_8)
                val payload = if kind == "error" then headers.getOrElse(":error-message", rawPayload) else rawPayload
                events += Event(kind, name, payload)
              case (Left(error), _) => return Left(error)
              case (_, Left(error)) => return Left(error)
        position += totalLength
      Right((events.result(), if position < bytes.length then Chunk.fromArray(bytes.drop(position)) else Chunk.empty))

    private def parseHeaders(bytes: Array[Byte], start: Int, end: Int): Either[Error, Map[String, String]] =
      val result = scala.collection.mutable.Map.empty[String, String]
      var position = start
      while position < end do
        val nameLength = bytes(position) & 0xff
        position += 1
        if nameLength == 0 || position + nameLength + 1 > end then
          return Left(protocolError("malformed AWS EventStream header name"))
        val name = new String(bytes, position, nameLength, java.nio.charset.StandardCharsets.UTF_8)
        position += nameLength
        if result.contains(name) then return Left(protocolError(s"duplicate AWS EventStream header: $name"))
        val headerType = bytes(position) & 0xff
        position += 1
        val valueLength = headerType match
          case 0 | 1 => 0
          case 2     => 1
          case 3     => 2
          case 4     => 4
          case 5 | 8 => 8
          case 9     => 16
          case 6 | 7 =>
            if position + 2 > end then return Left(protocolError(s"malformed AWS EventStream header: $name"))
            val length = ((bytes(position) & 0xff) << 8) | (bytes(position + 1) & 0xff)
            position += 2
            length
          case other => return Left(protocolError(s"unsupported AWS EventStream header type: $other"))
        if position + valueLength > end then return Left(protocolError(s"truncated AWS EventStream header: $name"))
        if headerType == 7 then
          result(name) = new String(bytes, position, valueLength, java.nio.charset.StandardCharsets.UTF_8)
        position += valueLength
      Right(result.toMap)

    private def protocolError(message: String): Error =
      Error.Unexpected(Status.InternalServerError, message)

    private def crc32(bytes: Array[Byte], offset: Int, length: Int): Long =
      val checksum = new java.util.zip.CRC32
      checksum.update(bytes, offset, length)
      checksum.getValue

    private def unsigned(value: Int): Long = java.lang.Integer.toUnsignedLong(value)

    private def readInt(bytes: Array[Byte], offset: Int): Int =
      ((bytes(offset) & 0xff) << 24) |
        ((bytes(offset + 1) & 0xff) << 16) |
        ((bytes(offset + 2) & 0xff) << 8) |
        (bytes(offset + 3) & 0xff)
