package com.jamesward.zio_bedrock.internal

import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.*
import com.jamesward.zio_bedrock.internal.Codecs.given
import zio.*
import zio.schema.{DynamicValue, Schema}
import zio.stream.*

private[zio_bedrock] object LoopImpl:


  def runDynamicLoop[R, E](
    prompt: String,
    tools: List[Tool[?]],
    handler: (ToolName, ToolInput) => ZIO[R, E, DynamicToolResult],
    systemMessage: String | Null,
    inferenceConfig: InferenceConfig | Null,
    maxTurns: Int,
  ): ZIO[Bedrock & R, Error | E, DynamicLoopResult[Output]] =
    def optionalInt(value: Int | Null): Option[Int] =
      if value.asInstanceOf[AnyRef] eq null then None else Some(value.asInstanceOf[Int])

    def totals(turns: List[LoopTurn]): LoopTotals =
      def optionalLong(value: Long | Null): Option[Long] =
        if value.asInstanceOf[AnyRef] eq null then None else Some(value.asInstanceOf[Long])
      val usages = turns.map(_.usage)
      val cacheReads = usages.flatMap(usage => optionalInt(usage.cacheReadInputTokens))
      val cacheWrites = usages.flatMap(usage => optionalInt(usage.cacheWriteInputTokens))
      LoopTotals(
        TokenUsage(
          inputTokens = usages.map(_.inputTokens).sum,
          outputTokens = usages.map(_.outputTokens).sum,
          totalTokens = usages.map(_.totalTokens).sum,
          cacheReadInputTokens = if cacheReads.isEmpty then null else cacheReads.sum,
          cacheWriteInputTokens = if cacheWrites.isEmpty then null else cacheWrites.sum,
        ),
        latencyMs =
          val latencies = turns.flatMap(turn => optionalLong(turn.metrics.latencyMs))
          if latencies.size == turns.size then latencies.sum else null,
      )

    val initial = Tools.toWire(
      RequestConfig(
        messages = List(Message.user(prompt)),
        system = systemMessage,
        inferenceConfig = inferenceConfig,
        toolConfig = ToolConfig(tools),
      ),
      outputConfig = None,
    )

    ZIO.serviceWithZIO[Bedrock]: client =>
      def step(
        messages: List[Wire.WireMessage],
        turn: Int,
        previousTurns: List[LoopTurn],
      ): ZIO[R, Error | E, DynamicLoopResult[Output]] =
        if turn > maxTurns then ZIO.fail(Error.MaxIterations(maxTurns))
        else
          client.protocol.send(initial.copy(messages = messages)).flatMap: response =>
            val toolUses = response.output.message.content.collect:
              case Wire.ContentBlock.ToolUse(value) => value
            val current = LoopTurn(
              turn,
              response.stopReason,
              response.usage,
              response.metrics,
              toolUses.map(_.name),
            )
            val allTurns = previousTurns :+ current
            ZIO.logDebug(
              s"[Bedrock.dynamicLoop] turn=$turn stop=${response.stopReason} tools=[${current.toolNames.map(ToolName.unwrap).mkString(", ")}]"
            ) *>
              (if toolUses.isEmpty then
                 ZIO.succeed(DynamicLoopResult(
                   Output(Message.fromWire(response.output.message)),
                   response.stopReason,
                   allTurns,
                   totals(allTurns),
                 ))
               else
                 ZIO.foreachPar(toolUses) { toolUse =>
                   handler(toolUse.name, new ToolInput(toolUse.input)).map: result =>
                     Helpers.toWireContentBlock(ContentBlock.ToolResult(
                       toolUse.toolUseId,
                       result.content,
                       result.status,
                     ))
                 }.withParallelism(8).flatMap: results =>
                   val resultMessage = Wire.WireMessage(Role.User, results)
                   step(messages :+ response.output.message :+ resultMessage, turn + 1, allTurns))

      step(initial.messages, turn = 1, previousTurns = Nil)
  def runLoop(
    lr:           LoopRequest[?],
    outputConfig: Option[Wire.OutputConfig],
  ): ZIO[Bedrock, Error, Wire.ChatResponse] =
    ZIO.serviceWithZIO[Bedrock]: client =>
      val (wireToolConfig, wireSystem, wireInference, initialMessages) = buildBase(lr)

      def step(messages: List[Wire.WireMessage], iterations: Int): ZIO[Any, Error, Wire.ChatResponse] =
        if iterations > lr.maxIter then ZIO.fail(Error.MaxIterations(iterations - 1))
        else
          val wireReq = Wire.ChatRequest(messages, wireSystem, wireInference, wireToolConfig, outputConfig)
          client.protocol.send(wireReq).flatMap: wire =>
            val toolUses = wire.output.message.content.collect { case Wire.ContentBlock.ToolUse(tu) => tu }
            if toolUses.nonEmpty then
              val toolNames = toolUses.map(_.name).mkString(", ")
              ZIO.logDebug(s"[Bedrock.loop] iteration=$iterations tool_calls=[$toolNames]") *>
              ZIO.foreachPar(toolUses)(dispatchTool(lr, _)).withParallelism(8).flatMap: results =>
                val toolResultMsg = Wire.WireMessage(Role.User, results.toList.map(Wire.ContentBlock.ToolResult.apply))
                step(messages :+ wire.output.message :+ toolResultMsg, iterations + 1)
            else
              ZIO.logDebug(s"[Bedrock.loop] iteration=$iterations reply_complete") *>
              ZIO.succeed(wire)

      step(initialMessages, 1)

  def runLoopEventStream(
    lr:           LoopRequest[?],
    outputConfig: Option[Wire.OutputConfig],
  ): ZStream[Bedrock, Error, StreamEvent] =
    ZStream.unwrap:
      ZIO.serviceWith[Bedrock]: client =>
        val (wireToolConfig, wireSystem, wireInference, initialMessages) = buildBase(lr)

        def streamStep(messages: List[Wire.WireMessage], iterations: Int): ZStream[Any, Error, StreamEvent] =
          if iterations > lr.maxIter then ZStream.fail(Error.MaxIterations(iterations - 1))
          else
            val wireReq = Wire.ChatRequest(messages, wireSystem, wireInference, wireToolConfig, outputConfig)
            val eventsStream = client.protocol.completedStreamEvents(wireReq)
            ZStream.unwrap:
              Ref.make(Option.empty[StreamComplete]).map: completeRef =>
                val eventsWithoutComplete = eventsStream.mapZIO:
                  case StreamEvent.Complete(value) => completeRef.set(Some(value)).as(None)
                  case event                       => ZIO.succeed(Some(event))
                .collect { case Some(event) => event }

                eventsWithoutComplete ++ ZStream.unwrap:
                  completeRef.get.flatMap:
                    case None => ZIO.fail(Error.Unexpected(zio.http.Status.InternalServerError, "stream ended without a complete result"))
                    case Some(completion) =>
                      val response = completion.response
                      val toolUses = response.output.message.content.collect { case Wire.ContentBlock.ToolUse(value) => value }
                      if toolUses.isEmpty then ZIO.succeed(ZStream.succeed(StreamEvent.Complete(completion)))
                      else
                        ZIO.logDebug(s"[Bedrock.loop.asStream] iteration=$iterations tool_calls=[${toolUses.map(_.name).mkString(", ")}]") *>
                        ZIO.foreachPar(toolUses)(dispatchTool(lr, _)).withParallelism(8).map: results =>
                          val toolResultMsg = Wire.WireMessage(Role.User, results.toList.map(Wire.ContentBlock.ToolResult.apply))
                          streamStep(messages :+ response.output.message :+ toolResultMsg, iterations + 1)

        streamStep(initialMessages, 1)

  def runLoopStream(
    lr:           LoopRequest[?],
    outputConfig: Option[Wire.OutputConfig],
  ): ZStream[Bedrock, Error, String] =
    ZStream.unwrap:
      ZIO.serviceWith[Bedrock]: client =>
        val (wireToolConfig, wireSystem, wireInference, initialMessages) = buildBase(lr)

        def streamStep(messages: List[Wire.WireMessage], iterations: Int): ZStream[Any, Error, String] =
          if iterations > lr.maxIter then ZStream.fail(Error.MaxIterations(iterations - 1))
          else
            val wireReq = Wire.ChatRequest(messages, wireSystem, wireInference, wireToolConfig, outputConfig)
            val eventsStream = client.protocol.completedStreamEvents(wireReq)
            ZStream.fromZIO(eventsStream.runCollect).flatMap: events =>
              events.collectFirst { case StreamEvent.Complete(value) => value.response } match
                case None => ZStream.fail(Error.Unexpected(zio.http.Status.InternalServerError, "stream ended without a complete result"))
                case Some(response) =>
                  val toolUses = response.output.message.content.collect { case Wire.ContentBlock.ToolUse(value) => value }
                  if toolUses.isEmpty then
                    ZStream.fromIterable(events.collect { case StreamEvent.TextDelta(text) => text })
                  else
                    ZStream.fromZIO(
                      ZIO.logDebug(s"[Bedrock.loop.stream] iteration=$iterations tool_calls=[${toolUses.map(_.name).mkString(", ")}]") *>
                      ZIO.foreachPar(toolUses)(dispatchTool(lr, _)).withParallelism(8)
                    ).flatMap: results =>
                      val toolResultMsg = Wire.WireMessage(Role.User, results.toList.map(Wire.ContentBlock.ToolResult.apply))
                      streamStep(messages :+ response.output.message :+ toolResultMsg, iterations + 1)

        streamStep(initialMessages, 1)

  // ── shared helpers ──

  private def buildBase(lr: LoopRequest[?]) =
    val wireToolDefs: List[Wire.ToolDef] = lr.handlers.toList.map: (name, h) =>
      Wire.ToolDef.ToolSpec(Wire.ToolSpecData(name, Some(h.description), None, h.inputSchema))
    val wireToolConfig =
      if wireToolDefs.isEmpty then None
      else Some(Wire.ToolConfig(wireToolDefs, Some(Wire.ToolChoice.Auto(Wire.EmptyObject()))))
    val wireSystem: List[Wire.SystemContentBlock] = lr.systemMsg match
      case null => Nil
      case s    => List(Wire.SystemContentBlock.Text(s))
    val wireInference: Option[InferenceConfig] =
      if lr.infCfg.asInstanceOf[AnyRef] eq null then None else Some(lr.infCfg.asInstanceOf[InferenceConfig])
    val initialMessages = List(Wire.WireMessage(Role.User, List(Wire.ContentBlock.Text(lr.prompt))))
    (wireToolConfig, wireSystem, wireInference, initialMessages)

  def dispatchTool(lr: LoopRequest[?], tu: Wire.ToolUseContent): ZIO[Any, Nothing, Wire.ToolResultContent] =
    lr.handlers.get(tu.name) match
      case None =>
        ZIO.logDebug(s"[Bedrock.loop] dispatch unknown tool=${tu.name}") *>
        ZIO.succeed(errorResult(tu.toolUseId, s"Unknown tool: ${tu.name}"))
      case Some(handler) =>
        val unwrapped = unwrapIfPrimitive(handler.inputSchema, tu.input)
        handler.inputSchema.asInstanceOf[Schema[Any]].fromDynamic(unwrapped) match
          case Left(err) =>
            ZIO.logDebug(s"[Bedrock.loop] dispatch tool=${tu.name} invalid_input") *>
            ZIO.succeed(errorResult(tu.toolUseId, s"Invalid input: $err"))
          case Right(typedInput) =>
            ZIO.logDebug(s"[Bedrock.loop] dispatch tool=${tu.name} input_decoded") *> {
            val handlerErased = handler.handler.asInstanceOf[Any => ZIO[Any, Any, Any]]
            val outSchema     = handler.outputSchema.asInstanceOf[Schema[Any]]
            val errSchema     = handler.errorSchema.asInstanceOf[Schema[Any]]
            handlerErased(typedInput).foldZIO(
              e =>
                ZIO.logDebug(s"[Bedrock.loop] dispatch tool=${tu.name} handler_failed").as:
                  Wire.ToolResultContent(tu.toolUseId, List(Wire.ToolResultBlock.Json(wrapIfPrimitive(errSchema, errSchema.toDynamic(e)))), Some(Wire.ToolResultStatus.Error)),
              a =>
                ZIO.logDebug(s"[Bedrock.loop] dispatch tool=${tu.name} handler_succeeded").as:
                  Wire.ToolResultContent(tu.toolUseId, List(Wire.ToolResultBlock.Json(wrapIfPrimitive(outSchema, outSchema.toDynamic(a)))), Some(Wire.ToolResultStatus.Success)),
            )}

  private def errorResult(toolUseId: ToolUseId, msg: String): Wire.ToolResultContent =
    Wire.ToolResultContent(toolUseId, List(Wire.ToolResultBlock.Text(msg)), Some(Wire.ToolResultStatus.Error))

  /** If the schema is not a record, wrap the DynamicValue in a Record with a "value" field
    * so Bedrock accepts it as a JSON object. */
  private def wrapIfPrimitive(schema: Schema[?], dv: DynamicValue): DynamicValue =
    schema match
      case _: Schema.Record[?] => dv
      case _ => DynamicValue.Record(
        zio.schema.TypeId.Structural,
        scala.collection.immutable.ListMap("value" -> dv),
      )

  /** If the handler's input schema is not a record (i.e. primitive/enum/sequence),
    * the wire wraps it in {"value": ...}. Unwrap the DynamicValue accordingly. */
  private def unwrapIfPrimitive(schema: Schema[?], dv: DynamicValue): DynamicValue =
    schema match
      case _: Schema.Record[?] => dv
      case _ =>
        dv match
          case DynamicValue.Record(_, fields) =>
            fields.collectFirst { case ("value", v) => coercePrimitive(schema, v) }.getOrElse(dv)
          case _ => dv

  /** JSON decodes numbers as bigDecimal. Coerce to the target primitive type. */
  private def coercePrimitive(schema: Schema[?], dv: DynamicValue): DynamicValue =
    import zio.schema.{StandardType as ST}
        given CanEqual[ST[?], ST[?]] = CanEqual.derived
    (schema, dv) match
      case (p: Schema.Primitive[?], DynamicValue.Primitive(v: java.math.BigDecimal, _)) =>
        val st = p.standardType.asInstanceOf[ST[?]]
        if st == ST.IntType then DynamicValue.Primitive(v.intValue, ST.IntType)
        else if st == ST.LongType then DynamicValue.Primitive(v.longValue, ST.LongType)
        else if st == ST.DoubleType then DynamicValue.Primitive(v.doubleValue, ST.DoubleType)
        else if st == ST.FloatType then DynamicValue.Primitive(v.floatValue, ST.FloatType)
        else if st == ST.ShortType then DynamicValue.Primitive(v.shortValue, ST.ShortType)
        else dv
      case _ => dv
