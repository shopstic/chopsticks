package dev.chopsticks.alertmanager

import enumeratum.{CirceEnum, EnumEntry}
import enumeratum.EnumEntry.Lowercase
import io.circe.Decoder

import java.time.Instant

sealed abstract class AlertmanagerStatus extends EnumEntry with Lowercase
object AlertmanagerStatus extends enumeratum.Enum[AlertmanagerStatus] with CirceEnum[AlertmanagerStatus] {
  case object Firing extends AlertmanagerStatus
  case object Resolved extends AlertmanagerStatus
  //noinspection TypeAnnotation
  override val values = findValues
}

final case class AlertmanagerAlert(
  status: AlertmanagerStatus,
  labels: Map[String, String],
  annotations: Map[String, String],
  startsAt: Instant,
  endsAt: Instant,
  generatorURL: String,
  fingerprint: String,
  silenceURL: Option[String] = None, // Extended from Grafana only
  dashboardURL: Option[String] = None, // Extended from Grafana only
  panelURL: Option[String] = None, // Extended from Grafana only
  valueString: Option[String] = None // Extended from Grafana only
)
object AlertmanagerAlert {
  implicit val circeDecoder: Decoder[AlertmanagerAlert] = {
    import io.circe.generic.semiauto._
    deriveDecoder
  }
}

final case class AlertmanagerWebhookPayload(
  version: String,
  receiver: String,
  groupKey: String,
  truncatedAlerts: Int,
  status: AlertmanagerStatus,
  groupLabels: Map[String, String],
  commonLabels: Map[String, String],
  commonAnnotations: Map[String, String],
  externalURL: String,
  alerts: Seq[AlertmanagerAlert],
  orgId: Option[Long] = None // Extended from Grafana only
)
object AlertmanagerWebhookPayload {
  implicit val circeDecoder: Decoder[AlertmanagerWebhookPayload] = {
    import io.circe.generic.semiauto._
    deriveDecoder
  }
}
