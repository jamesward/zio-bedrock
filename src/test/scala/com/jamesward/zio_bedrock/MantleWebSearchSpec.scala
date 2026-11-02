package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.*
import zio.*
import zio.http.{Client as HttpClient, Request as HttpRequest, *}
import zio.json.*
import zio.json.ast.Json
import zio.test.*

/** Web Search through the Responses API on a local stand-in for bedrock-mantle: the request it sends, the answer it reads. */
object MantleWebSearchSpec extends ZIOSpecDefault:

  private val answer =
    """{"id":"resp_1","status":"completed","output":[
      |{"type":"web_search_call","id":"ws_1"},
      |{"type":"message","content":[{"type":"output_text","text":"Scala 3.10.0 is the latest.",
      |  "annotations":[{"type":"url_citation","url":"https://scala-lang.org/news/3.10/","title":"Scala 3.10","start_index":0,"end_index":12}]}]}]}""".stripMargin

  def spec = suite("Mantle web search")(
    test("posts the web_search tool to /openai/v1/responses, without external access or storage, and reads the cited answer") {
      for
        seen   <- Promise.make[Nothing, (String, String, Option[String])]
        routes  = Routes(
                    Method.POST / "openai" / "v1" / "responses" -> handler { (request: HttpRequest) =>
                      request.body.asString.orDie.flatMap(body =>
                        seen.succeed((request.url.path.encode, body, request.header(Header.Authorization).map(_.renderedValue))) *>
                          ZIO.succeed(Response.json(answer)))
                    },
                  )
        port   <- Server.install(routes)
        url     = URL.decode(s"http://127.0.0.1:$port").toOption.get
        got    <- MantleWebSearch.search(MantleConfig(ApiKey("k"), url, ModelId("openai.gpt-5.6-terra")), "latest Scala 3?",
                    Some(MantleWebSearch.ContextSize.Low))
        sent   <- seen.await
        body    = sent._2.fromJson[Json].toOption.get.toJson
      yield assertTrue(
        got.text == "Scala 3.10.0 is the latest.",
        got.citations == List(MantleWebSearch.Citation("https://scala-lang.org/news/3.10/", "Scala 3.10", 0, 12)),
        sent._1 == "/openai/v1/responses", sent._3.contains("Bearer k"),
        body.contains("\"type\":\"web_search\""), body.contains("\"external_web_access\":false"), body.contains("\"store\":false"),
        body.contains("\"search_context_size\":\"low\""), body.contains("openai.gpt-5.6-terra"),
      )
    },
    test("an error answer is Bedrock's error; supported models and Regions") {
      for
        port <- Server.install(Routes(Method.POST / "openai" / "v1" / "responses" -> handler(Response.status(Status.Forbidden))))
        url   = URL.decode(s"http://127.0.0.1:$port").toOption.get
        err  <- MantleWebSearch.search(MantleConfig(ApiKey("k"), url, ModelId("openai.gpt-5.4")), "q").flip
      yield assertTrue(
        err.toString.contains("403") || err.getClass.getSimpleName.contains("Access"),
        MantleWebSearch.supports(Region.UsWest2, ModelId("openai.gpt-5.4")), MantleWebSearch.supports(Region.UsEast1, ModelId("openai.gpt-6.0")),
        !MantleWebSearch.supports(Region.UsWest2, ModelId("openai.gpt-oss-120b")), !MantleWebSearch.supports(Region.UsWest2, ModelId("openai.gpt-5.2")),
        !MantleWebSearch.supports(Region.EuWest1, ModelId("openai.gpt-5.4")),
      )
    },
  ).provide(Server.defaultWithPort(0), HttpClient.default) @@ TestAspect.withLiveClock
