package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId, Region}
import com.jamesward.zio_bedrock.internal.MantleTransport
import zio.*
import zio.http.{Client, URL}

/** Configuration for the Bedrock Mantle Chat Completions backend.
  * `endpoint`, when supplied, replaces the regional AWS origin while retaining
  * the standard `/v1/chat/completions` path.
  */
final case class MantleConfig(
  apiKey: ApiKey,
  region: Region,
  modelId: ModelId,
  endpoint: URL | Null = null,
):
  override def toString: String =
    val endpointDescription = if endpoint.asInstanceOf[AnyRef] eq null then "<regional>" else "<custom>"
    s"MantleConfig(<redacted>, $region, ${modelId.unwrap}, $endpointDescription)"

object MantleConfig:
  /** Configuration for a Mantle-compatible custom origin. */
  def apply(apiKey: ApiKey, endpoint: URL, modelId: ModelId): MantleConfig =
    new MantleConfig(apiKey, Region.UsEast1, modelId, endpoint)

/** Failure while loading [[MantleConfig]] from the process environment. */
sealed trait MantleConfigError extends Throwable:
  def errorMessage: String
  override final def getMessage: String = errorMessage

object MantleConfigError:
  case object MissingApiKey extends MantleConfigError:
    val errorMessage = "Missing API key (set AWS_BEARER_TOKEN_BEDROCK)"

  case object MissingModelId extends MantleConfigError:
    val errorMessage = "Missing model ID (set BEDROCK_MODEL_ID)"

  final case class InvalidRegion(code: String) extends MantleConfigError:
    val errorMessage = s"Invalid AWS region: $code"

/** Layers for the Bedrock Mantle backend. */
object Mantle:

  /** Build Mantle from explicit configuration and an existing ZIO HTTP client. */
  def layer(config: MantleConfig): ZLayer[Client, Nothing, Bedrock] =
    ZLayer.fromFunction((client: Client) => MantleTransport.build(config, client))

  /** Load Mantle configuration from the environment and use an existing HTTP client.
    * `AWS_BEARER_TOKEN_BEDROCK` and `BEDROCK_MODEL_ID` are required; `AWS_REGION`
    * defaults to `us-east-1`.
    */
  val configured: ZLayer[Client, MantleConfigError, Bedrock] =
    ZLayer.fromZIO:
      for
        apiKey <- System.env("AWS_BEARER_TOKEN_BEDROCK").orDie
          .someOrFail(MantleConfigError.MissingApiKey)
          .filterOrFail(_.nonEmpty)(MantleConfigError.MissingApiKey)
        modelId <- System.env("BEDROCK_MODEL_ID").orDie
          .someOrFail(MantleConfigError.MissingModelId)
          .filterOrFail(_.nonEmpty)(MantleConfigError.MissingModelId)
        regionCode <- System.env("AWS_REGION").orDie.map(_.filter(_.nonEmpty).getOrElse(Region.UsEast1.code))
        region <- ZIO.fromOption(Region.fromCode(regionCode))
          .orElseFail(MantleConfigError.InvalidRegion(regionCode))
        client <- ZIO.service[Client]
      yield MantleTransport.build(MantleConfig(ApiKey(apiKey), region, ModelId(modelId)), client)

  /** Self-contained live Mantle backend, including ZIO HTTP's default client. */
  val live: ZLayer[Any, Throwable, Bedrock] =
    Client.default >>> configured
