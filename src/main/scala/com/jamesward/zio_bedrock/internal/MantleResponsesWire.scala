package com.jamesward.zio_bedrock.internal

import com.jamesward.zio_bedrock.Bedrock.ModelId
import zio.schema.annotation.{caseName, fieldName}
import zio.schema.{Schema, derived}

/** OpenAI Responses API shapes used by Bedrock Mantle's server-side web search
  * (https://docs.aws.amazon.com/bedrock/latest/userguide/web-search.html).
  * Only the fields web search needs are modelled; other fields and output item
  * types (such as `web_search_call` and `reasoning`) are ignored when decoding.
  */
private[zio_bedrock] object MantleResponsesWire:

  /** Path of the Responses API on bedrock-mantle. Web search is served only there, under `/openai/v1`. */
  val path: String = "/openai/v1/responses"

  /** How many observations each Search call may return to the model (low: 5, medium: 11, high: 25). */
  enum SearchContextSize derives Schema:
    @caseName("low") case Low
    @caseName("medium") case Medium
    @caseName("high") case High

  // `external_web_access` defaults to true on the service, which makes every Fetch fail authorization under the
  // general Bedrock policies (none grant bedrock-websearch:ExternalWebAccess) while the request still returns 200.
  // Sending false keeps retrieval in the Bedrock index and cache, inside the AWS boundary.
  final case class WebSearchTool(
    `type`: String = "web_search",
    @fieldName("external_web_access") externalWebAccess: Boolean = false,
    @fieldName("search_context_size") searchContextSize: Option[SearchContextSize] = None,
  ) derives Schema

  // `store` defaults to true on the service, which keeps the request and response.
  final case class Request(
    model: String,
    input: String,
    tools: List[WebSearchTool],
    store: Boolean = false,
  ) derives Schema

  final case class Annotation(
    `type`: String,
    url: Option[String] = None,
    title: Option[String] = None,
    @fieldName("start_index") startIndex: Option[Int] = None,
    @fieldName("end_index") endIndex: Option[Int] = None,
  ) derives Schema

  final case class ContentPart(
    `type`: String,
    text: Option[String] = None,
    annotations: List[Annotation] = Nil,
  ) derives Schema

  final case class OutputItem(
    `type`: String,
    content: List[ContentPart] = Nil,
  ) derives Schema

  final case class Response(
    id: String,
    status: Option[String] = None,
    output: List[OutputItem] = Nil,
  ) derives Schema

  /** A `url_citation` annotation with every field the docs say it carries. */
  final case class UrlCitation(url: String, title: String, startIndex: Int, endIndex: Int)

  /** The answer text and its citations, from the `output_text` parts of the `message` items. */
  final case class Answer(text: String, citations: List[UrlCitation])

  def request(query: String, modelId: ModelId, searchContextSize: Option[SearchContextSize]): Request =
    Request(
      model = modelId.unwrap,
      input = query,
      tools = List(WebSearchTool(searchContextSize = searchContextSize)),
    )

  def answer(response: Response): Either[String, Answer] =
    val parts = response.output
      .filter(_.`type` == "message")
      .flatMap(_.content)
      .filter(_.`type` == "output_text")
    val texts = parts.flatMap(_.text)
    if texts.isEmpty then
      Left(s"Responses API returned no output_text (id=${response.id}, status=${response.status.fold("<none>")(identity)})")
    else
      val citations = parts.flatMap(_.annotations).filter(_.`type` == "url_citation").flatMap: annotation =>
        for
          url <- annotation.url
          title <- annotation.title
          start <- annotation.startIndex
          end <- annotation.endIndex
        yield UrlCitation(url, title, start, end)
      Right(Answer(texts.mkString, citations))
