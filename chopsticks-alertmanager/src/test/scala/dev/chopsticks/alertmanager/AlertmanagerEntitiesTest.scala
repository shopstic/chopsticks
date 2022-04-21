package dev.chopsticks.alertmanager

import dev.chopsticks.alertmanager.AlertmanagerStatus.{Firing, Resolved}
import org.scalatest.Assertions
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Instant

final class AlertmanagerEntitiesTest extends AnyWordSpecLike with Assertions with Matchers {
  "decode Prometheus Alertmanager webhook payload" in {
    import io.circe.parser._

    val payload = """
       |{
       |  "version": "4",
       |  "groupKey": "foobar",
       |  "truncatedAlerts": 123,
       |  "status": "firing",
       |  "receiver": "foo",
       |  "groupLabels": {"group1":"group1","group2":"group2"},
       |  "commonLabels": {"label1":"label1","label2":"label2"},
       |  "commonAnnotations": {"anno1":"anno1","anno2":"anno2"},
       |  "externalURL": "http://foo.baz.com/some/path?query=here",
       |  "alerts": [
       |    {
       |      "status": "resolved",
       |      "labels": {"label1":"label1","label2":"label2"},
       |      "annotations": {"anno1":"anno1","anno2":"anno2"},
       |      "startsAt": "0001-01-01T00:00:00Z",
       |      "endsAt": "2022-04-21T02:22:01.056032065Z",
       |      "generatorURL": "http://foo.baz.com/some/path?query=here",
       |      "fingerprint": "d5fd0a851c27c0f3"
       |    }
       |  ]
       |}
       |""".stripMargin

    inside(decode[AlertmanagerWebhookPayload](payload)) {
      case Right(payload) =>
        payload should equal(
          AlertmanagerWebhookPayload(
            version = "4",
            groupKey = "foobar",
            truncatedAlerts = 123,
            status = Firing,
            receiver = "foo",
            groupLabels = Map("group1" -> "group1", "group2" -> "group2"),
            commonLabels = Map("label1" -> "label1", "label2" -> "label2"),
            commonAnnotations = Map("anno1" -> "anno1", "anno2" -> "anno2"),
            externalURL = "http://foo.baz.com/some/path?query=here",
            alerts = Seq(
              AlertmanagerAlert(
                status = Resolved,
                labels = Map("label1" -> "label1", "label2" -> "label2"),
                annotations = Map("anno1" -> "anno1", "anno2" -> "anno2"),
                startsAt = Instant.parse("0001-01-01T00:00:00Z"),
                endsAt = Instant.parse("2022-04-21T02:22:01.056032065Z"),
                generatorURL = "http://foo.baz.com/some/path?query=here",
                fingerprint = "d5fd0a851c27c0f3"
              )
            )
          )
        )
    }
  }

  "decode Grafana Alertmanager webhook payload" in {
    import io.circe.parser._

    val payload = """
      |{
      |  "version": "1",
      |  "groupKey": "foobar",
      |  "truncatedAlerts": 123,
      |  "status": "firing",
      |  "receiver": "foo",
      |  "groupLabels": {"group1":"group1","group2":"group2"},
      |  "commonLabels": {"label1":"label1","label2":"label2"},
      |  "commonAnnotations": {"anno1":"anno1","anno2":"anno2"},
      |  "externalURL": "http://foo.baz.com/some/path?query=here",
      |  "alerts": [
      |    {
      |      "status": "resolved",
      |      "labels": {"label1":"label1","label2":"label2"},
      |      "annotations": {"anno1":"anno1","anno2":"anno2"},
      |      "startsAt": "0001-01-01T00:00:00Z",
      |      "endsAt": "2022-04-21T02:22:01.056032065Z",
      |      "generatorURL": "http://foo.baz.com/some/path?query=here",
      |      "fingerprint": "d5fd0a851c27c0f3",
      |      "silenceURL": "http://foo.baz.com/some/silence?query=here",
      |      "dashboardURL": "http://foo.baz.com/some/dashboard?query=here",
      |      "panelURL": "http://foo.baz.com/some/panel?query=here",
      |      "valueString": "[ metric='vector(1)' labels={} value=1 ]"
      |    }
      |  ],
      |  "orgId": 7637437834,
      |  "title": "Deprecated",
      |  "state": "alerting",
      |  "message": "Deprecated"
      |}
      |""".stripMargin

    inside(decode[AlertmanagerWebhookPayload](payload)) {
      case Right(payload) =>
        payload should equal(
          AlertmanagerWebhookPayload(
            version = "1",
            groupKey = "foobar",
            truncatedAlerts = 123,
            status = Firing,
            receiver = "foo",
            groupLabels = Map("group1" -> "group1", "group2" -> "group2"),
            commonLabels = Map("label1" -> "label1", "label2" -> "label2"),
            commonAnnotations = Map("anno1" -> "anno1", "anno2" -> "anno2"),
            externalURL = "http://foo.baz.com/some/path?query=here",
            alerts = Seq(
              AlertmanagerAlert(
                status = Resolved,
                labels = Map("label1" -> "label1", "label2" -> "label2"),
                annotations = Map("anno1" -> "anno1", "anno2" -> "anno2"),
                startsAt = Instant.parse("0001-01-01T00:00:00Z"),
                endsAt = Instant.parse("2022-04-21T02:22:01.056032065Z"),
                generatorURL = "http://foo.baz.com/some/path?query=here",
                fingerprint = "d5fd0a851c27c0f3",
                silenceURL = Some("http://foo.baz.com/some/silence?query=here"),
                dashboardURL = Some("http://foo.baz.com/some/dashboard?query=here"),
                panelURL = Some("http://foo.baz.com/some/panel?query=here"),
                valueString = Some("[ metric='vector(1)' labels={} value=1 ]")
              )
            ),
            orgId = Some(7637437834L)
          )
        )
    }
  }
}
