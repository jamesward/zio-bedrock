package com.jamesward.zio_bedrock.internal

import com.jamesward.zio_bedrock.{Bedrock, MantleConfig}
import com.jamesward.zio_bedrock.Bedrock.Error
import com.jamesward.zio_bedrock.internal.Codecs.given
import zio.*
import zio.http.*
import zio.schema.codec.JsonCodec
import zio.stream.*

/** HTTP transport for Bedrock Mantle's OpenAI-compatible Chat Completions API. */
private[zio_bedrock] object MantleTransport:

  def build(config: MantleConfig, client: Client): Bedrock =
    val endpoint = Option(config.endpoint.asInstanceOf[URL]).getOrElse:
      URL.decode(s"https://bedrock-mantle.${config.region.code}.api.aws").toOption.get
    val authedClient = client
      .url(endpoint)
      .addHeader(Header.Authorization.Bearer(config.apiKey.unwrap))
      .addHeader(Header.ContentType(MediaType.application.json))
    val transport = new MantleProtocol(config.modelId, s"${config.api.basePath}/chat/completions", authedClient)
    new Bedrock:
      private[zio_bedrock] val protocol: Protocol = transport

  private final class MantleProtocol(
    modelId: Bedrock.ModelId,
    chatCompletionsPath: String,
    hc: Client,
  ) extends Protocol:

    private val requestCodec = JsonCodec.schemaBasedBinaryCodec[MantleWire.Request](Codecs.codecConfig)
    private val responseCodec = JsonCodec.schemaBasedBinaryCodec[MantleWire.Response](Codecs.codecConfig)
    private val chunkCodec = JsonCodec.schemaBasedBinaryCodec[MantleWire.Chunk](Codecs.codecConfig)

    def send(req: Wire.ChatRequest): IO[Error, Wire.ChatResponse] =
      val mantleRequest = MantleWire.request(req, modelId, streaming = false)
      val body = Body.fromChunk(requestCodec.encode(mantleRequest))
      ZIO.scoped:
        hc.post(chatCompletionsPath)(body)
          .mapError(Error.Transport.apply)
          .flatMap: response =>
            if response.status.isSuccess then
              response.body.asChunk
                .mapError(Error.Transport.apply)
                .flatMap: bytes =>
                  ZIO.fromEither(responseCodec.decode(bytes))
                    .mapError(error => Error.Unexpected(response.status, s"Decode failed: ${error.message}"))
                    .flatMap(decoded =>
                      ZIO.fromEither(MantleWire.response(decoded))
                        .mapError(message => Error.Unexpected(response.status, message)),
                    )
            else failResponse(response)

    def sendStreamEvents(req: Wire.ChatRequest): ZStream[Any, Error, Bedrock.StreamEvent] =
      val mantleRequest = MantleWire.request(req, modelId, streaming = true)
      val body = Body.fromChunk(requestCodec.encode(mantleRequest))
      ZStream.unwrapScoped:
        hc.addHeader(Header.Custom("Accept", "text/event-stream"))
          .post(chatCompletionsPath)(body)
          .mapError(Error.Transport.apply)
          .flatMap: response =>
            if response.status.isSuccess then
              ZIO.succeed:
                val accumulator = new MantleWire.StreamAccumulator
                val chunks = response.body.asStream
                  .mapError(Error.Transport.apply)
                  .via(ZPipeline.utf8Decode)
                  .via(ZPipeline.splitLines)
                  .mapError(Error.Transport.apply)
                  .collect:
                    case line if line.startsWith("data:") =>
                      line.stripPrefix("data:").stripPrefix(" ").trim
                  .filter(data => data.nonEmpty && data != "[DONE]")
                  .mapZIO: data =>
                    ZIO.fromEither(chunkCodec.decode(Chunk.fromArray(data.getBytes(java.nio.charset.StandardCharsets.UTF_8))))
                      .mapError(error => Error.Unexpected(response.status, s"SSE decode failed: ${error.message}; data=$data"))
                      .map(accumulator.add)
                  .mapConcat(identity)
                chunks ++ ZStream.fromZIO(
                  ZIO.fromEither(accumulator.complete)
                    .mapError(message => Error.Unexpected(response.status, s"SSE completion failed: $message")),
                )
            else failResponse(response)

    private def failResponse(response: Response): IO[Error, Nothing] =
      response.body.asString
        .mapError(Error.Transport.apply)
        .flatMap(text => ZIO.fail(Error.fromStatus(response.status, text)))
