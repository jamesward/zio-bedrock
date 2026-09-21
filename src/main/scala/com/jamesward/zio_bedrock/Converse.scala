package com.jamesward.zio_bedrock

import com.jamesward.zio_bedrock.Bedrock.{ApiKey, ModelId, Region}
import com.jamesward.zio_bedrock.internal.ConverseTransport
import zio.*
import zio.http.{Client, URL}

/** Configuration for the native Amazon Bedrock Converse backend.
  * `endpoint`, when supplied, replaces the regional Bedrock Runtime origin
  * while retaining the standard model-specific Converse paths.
  */
final case class ConverseConfig(
  apiKey: ApiKey,
  region: Region,
  modelId: ModelId,
  endpoint: URL | Null = null,
):
  override def toString: String =
    val endpointDescription = if endpoint.asInstanceOf[AnyRef] eq null then "<regional>" else "<custom>"
    s"ConverseConfig(<redacted>, $region, ${modelId.unwrap}, $endpointDescription)"

object ConverseConfig:
  /** Configuration for a Converse-compatible custom origin. */
  def apply(apiKey: ApiKey, endpoint: URL, modelId: ModelId): ConverseConfig =
    new ConverseConfig(apiKey, Region.UsEast1, modelId, endpoint)

/** Failure while loading [[ConverseConfig]] from the process environment. */
sealed trait ConverseConfigError extends Throwable:
  def errorMessage: String
  override final def getMessage: String = errorMessage

object ConverseConfigError:
  case object MissingApiKey extends ConverseConfigError:
    val errorMessage = "Missing API key (set AWS_BEARER_TOKEN_BEDROCK)"

  case object MissingModelId extends ConverseConfigError:
    val errorMessage = "Missing model ID (set BEDROCK_MODEL_ID)"

  final case class InvalidRegion(code: String) extends ConverseConfigError:
    val errorMessage = s"Invalid AWS region: $code"

/** Layers for the native Amazon Bedrock Converse backend. */
object Converse:

  /** Build Converse from explicit configuration and an existing ZIO HTTP client. */
  def layer(config: ConverseConfig): ZLayer[Client, Nothing, Bedrock] =
    ZLayer.fromFunction((client: Client) => ConverseTransport.build(config, client))

  /** Load Converse configuration from the environment and use an existing HTTP client.
    * `AWS_BEARER_TOKEN_BEDROCK` and `BEDROCK_MODEL_ID` are required; `AWS_REGION`
    * defaults to `us-east-1`.
    */
  val configured: ZLayer[Client, ConverseConfigError, Bedrock] =
    ZLayer.fromZIO:
      for
        apiKey <- System.env("AWS_BEARER_TOKEN_BEDROCK").orDie
          .someOrFail(ConverseConfigError.MissingApiKey)
          .filterOrFail(_.nonEmpty)(ConverseConfigError.MissingApiKey)
        modelId <- System.env("BEDROCK_MODEL_ID").orDie
          .someOrFail(ConverseConfigError.MissingModelId)
          .filterOrFail(_.nonEmpty)(ConverseConfigError.MissingModelId)
        regionCode <- System.env("AWS_REGION").orDie.map(_.filter(_.nonEmpty).getOrElse(Region.UsEast1.code))
        region <- ZIO.fromOption(Region.fromCode(regionCode))
          .orElseFail(ConverseConfigError.InvalidRegion(regionCode))
        client <- ZIO.service[Client]
      yield ConverseTransport.build(ConverseConfig(ApiKey(apiKey), region, ModelId(modelId)), client)

  /** Self-contained live Converse backend, including ZIO HTTP's default client. */
  val live: ZLayer[Any, Throwable, Bedrock] =
    Client.default >>> configured
