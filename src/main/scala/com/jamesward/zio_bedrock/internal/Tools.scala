package com.jamesward.zio_bedrock.internal

import com.jamesward.zio_bedrock.Bedrock
import com.jamesward.zio_bedrock.Bedrock.{InferenceConfig, RequestConfig, ToolChoice, ToolConfig}

/**
 * Wire translation for the public [[RequestConfig]].
 *
 * Used by low-level requests and as the initial normalized shape for
 * backend-neutral tool orchestration.
 */
private[zio_bedrock] object Tools:

  def toWire(
    cfg:          RequestConfig,
    outputConfig: Option[Wire.OutputConfig],
  ): Wire.ChatRequest =
    val messages = cfg.messages.map: m =>
      if m.retained.asInstanceOf[AnyRef] eq null then
        val wireContent = m.content.map(Helpers.toWireContentBlock)
        Wire.WireMessage(role = m.role, content = wireContent)
      else m.retained.asInstanceOf[Wire.WireMessage]
    val system = cfg.system match
      case null => Nil
      case s    => List(Wire.SystemContentBlock.Text(s))
    val inference =
      if cfg.inferenceConfig.asInstanceOf[AnyRef] eq null then None
      else Some(cfg.inferenceConfig.asInstanceOf[InferenceConfig])
    val tools =
      if cfg.toolConfig.asInstanceOf[AnyRef] eq null then None
      else Some(toWireToolConfig(cfg.toolConfig.asInstanceOf[ToolConfig]))
    Wire.ChatRequest(
      messages        = messages,
      system          = system,
      inferenceConfig = inference,
      toolConfig      = tools,
      outputConfig    = outputConfig,
    )

  private def toWireToolConfig(tc: ToolConfig): Wire.ToolConfig =
    Wire.ToolConfig(
      tools = tc.tools.map: t =>
        val base = (t.name, Some(t.description), Option.empty[Boolean])
        val spec = t.inputSchema match
          case Bedrock.Tool.SchemaSource.Typed(schema) =>
            Wire.ToolSpecData(base._1, base._2, base._3, schema)
          case Bedrock.Tool.SchemaSource.Dynamic(schema) =>
            Wire.ToolSpecData.dynamic(base._1, base._2, base._3, schema)
        Wire.ToolDef.ToolSpec(spec),
      toolChoice = Some(tc.toolChoice match
        case ToolChoice.Auto       => Wire.ToolChoice.Auto(Wire.EmptyObject())
        case ToolChoice.Any        => Wire.ToolChoice.Any(Wire.EmptyObject())
        case ToolChoice.Tool(name) => Wire.ToolChoice.Tool(Wire.ToolChoiceTool(name))
      ),
    )
