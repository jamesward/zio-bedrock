package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.*
import zio.*
import zio.http.{Client as HttpClient, Request as HttpRequest, *}
import zio.test.*
import zio.test.TestAspect.*

/** Compile- and runtime proof that one shared program can use either backend layer. */
object DualBackendSpec extends ZIOSpecDefault:
  private val model = ModelId("shared:model")

  private val program: ZIO[Bedrock, Bedrock.Error, String] =
    Bedrock.chat("same program").text

  private def layer(http: HttpClient, port: Int, converse: Boolean): ULayer[Bedrock] =
    val endpoint = URL.decode(s"http://localhost:$port").toOption.get
    val client = ZLayer.succeed(http)
    if converse then client >>> Converse.layer(ConverseConfig(ApiKey("dual-secret"), endpoint, model))
    else client >>> Mantle.layer(MantleConfig(ApiKey("dual-secret"), endpoint, model))

  def spec = suite("single artifact dual backend")(
    test("the same ZIO[Bedrock, ...] program runs with Mantle and Converse") {
      for
        requests <- Ref.make(List.empty[(String, Option[String])])
        routes = Routes(RoutePattern.any -> handler { (request: HttpRequest) =>
          requests.update(_ :+ (request.url.path.encode -> request.rawHeader("authorization"))) *>
            ZIO.succeed:
              if request.url.path.encode == "/v1/chat/completions" then
                Response.json("""{"id":"m","object":"chat.completion","created":1,"model":"shared:model","choices":[{"index":0,"message":{"role":"assistant","content":"mantle"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}""")
              else
                Response.json("""{"output":{"message":{"role":"assistant","content":[{"text":"converse"}]}},"stopReason":"end_turn","usage":{"inputTokens":1,"outputTokens":1,"totalTokens":2},"metrics":{"latencyMs":4}}""")
        })
        port <- Server.install(routes)
        http <- ZIO.service[HttpClient]
        mantle <- program.provideLayer(layer(http, port, converse = false))
        converse <- program.provideLayer(layer(http, port, converse = true))
        captured <- requests.get
      yield assertTrue(
        mantle == "mantle",
        converse == "converse",
        captured.map(_._1) == List("/v1/chat/completions", "/model/shared:model/converse"),
        captured.forall(_._2.contains("Bearer dual-secret")),
      )
    },
  ).provide(Server.defaultWith(_.onAnyOpenPort), HttpClient.default) @@ sequential @@ withLiveClock @@ timeout(30.seconds)
