package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.{Error, ModelId, Region}
import com.jamesward.zio_bedrock.internal.Codecs
import com.jamesward.zio_bedrock.internal.MantleResponsesWire
import zio.*
import zio.http.*
import zio.schema.codec.JsonCodec

/**
 * Amazon Bedrock's Web Search (https://docs.aws.amazon.com/bedrock/latest/userguide/web-search.html): a server-side
 * tool of the Responses API on `bedrock-mantle`, for OpenAI GPT-5.4 and later models in its Regions. A query is sent
 * with the `web_search` tool; the model searches (and fetches, from Bedrock's cache: `external_web_access` is sent
 * `false`, which the general Bedrock policies allow) and answers with `url_citation`s into its text. Nothing is
 * stored (`store` is `false`).
 */
object MantleWebSearch:

  /** How many observations each Search call may return to the model: up to 5, 11 (the service's default) or 25. */
  enum ContextSize derives CanEqual:
    case Low, Medium, High

  /** A source the answer cites: its URL and title, and the span of the answer's text it backs. */
  final case class Citation(url: String, title: String, start: Int, end: Int) derives CanEqual

  /** The model's answer and the sources it cites. */
  final case class Answer(text: String, citations: List[Citation])

  /** The Regions Web Search runs in. */
  val Regions: Set[Region] = Set(Region.UsEast1, Region.UsEast2, Region.UsWest2, Region.UsGovWest1)

  private val Gpt = """(?:[a-z]+\.)?openai\.gpt-(\d+)\.(\d+).*""".r

  /** Whether `model` in `region` has Web Search: an OpenAI GPT-5.4 or later model, in one of [[Regions]]. */
  def supports(region: Region, model: ModelId): Boolean =
    Regions(region) && (model.unwrap match
      case Gpt(major, minor) => major.toInt > 5 || (major.toInt == 5 && minor.toInt >= 4)
      case _                 => false)

  private val requestCodec = JsonCodec.schemaBasedBinaryCodec[MantleResponsesWire.Request](Codecs.codecConfig)
  private val responseCodec = JsonCodec.schemaBasedBinaryCodec[MantleResponsesWire.Response](Codecs.codecConfig)

  /** `query` answered by `config`'s model with Web Search, at `size` (the service's default when none). */
  def search(config: MantleConfig, query: String, size: Option[ContextSize] = None): ZIO[Client, Error, Answer] =
    val endpoint = Option(config.endpoint.asInstanceOf[URL]).getOrElse:
      URL.decode(s"https://bedrock-mantle.${config.region.code}.api.aws").toOption.get
    val wire = size.map:
      case ContextSize.Low    => MantleResponsesWire.SearchContextSize.Low
      case ContextSize.Medium => MantleResponsesWire.SearchContextSize.Medium
      case ContextSize.High   => MantleResponsesWire.SearchContextSize.High
    val body = Body.fromChunk(requestCodec.encode(MantleResponsesWire.request(query, config.modelId, wire)))
    ZIO.serviceWithZIO[Client]: client =>
      ZIO.scoped:
        client.url(endpoint)
          .addHeader(Header.Authorization.Bearer(config.apiKey.unwrap))
          .addHeader(Header.ContentType(MediaType.application.json))
          .post(MantleResponsesWire.path)(body)
          .mapError(Error.Transport.apply)
          .flatMap: response =>
            response.body.asChunk.mapError(Error.Transport.apply).flatMap: bytes =>
              if !response.status.isSuccess then ZIO.fail(Error.fromStatus(response.status, String(bytes.toArray, "UTF-8")))
              else
                ZIO.fromEither(responseCodec.decode(bytes))
                  .mapError(e => Error.Unexpected(response.status, s"Decode failed: ${e.message}"))
                  .flatMap(decoded => ZIO.fromEither(MantleResponsesWire.answer(decoded)).mapError(Error.Unexpected(response.status, _)))
                  .map(a => Answer(a.text, a.citations.map(c => Citation(c.url, c.title, c.startIndex, c.endIndex))))
