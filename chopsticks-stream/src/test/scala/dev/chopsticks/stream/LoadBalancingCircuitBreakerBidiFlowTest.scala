package dev.chopsticks.stream

import akka.stream.scaladsl.{Flow, Keep}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.testkit.ImplicitSender
import akka.util.Timeout
import dev.chopsticks.testkit.{AkkaTestKit, AkkaTestKitAutoShutDown}
import eu.timepit.refined.types.numeric.PosInt
import eu.timepit.refined.auto._
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.{Clock, Instant}
import scala.collection.compat.immutable.ArraySeq
import scala.concurrent.duration.DurationInt

object LoadBalancingCircuitBreakerBidiFlowTest {
  final private case class LoadBalancingCircuitBreakerBidiFlowTestProbes(
    requests: TestPublisher.Probe[String],
    requestsWithDestination: TestSubscriber.Probe[LoadBalancingCircuitBreakerRequest[String, String]],
    responsesWithDestination: TestPublisher.Probe[LoadBalancingCircuitBreakerResponse[
      LoadBalancingCircuitBreakerBidiFlowTestResponse,
      String
    ]],
    responses: TestSubscriber.Probe[LoadBalancingCircuitBreakerBidiFlowTestResponse]
  ) {
    def ensureSubscriptions() = {
      val _ = responses.ensureSubscription()
      val _ = responsesWithDestination.ensureSubscription()
      val _ = requestsWithDestination.ensureSubscription()
      val _ = requests.ensureSubscription()
      this
    }
  }

  sealed abstract private[LoadBalancingCircuitBreakerBidiFlowTest] class LoadBalancingCircuitBreakerBidiFlowTestResponse
  private[LoadBalancingCircuitBreakerBidiFlowTest] object LoadBalancingCircuitBreakerBidiFlowTestResponse {
    final case object Success extends LoadBalancingCircuitBreakerBidiFlowTestResponse
    final case object Timeout extends LoadBalancingCircuitBreakerBidiFlowTestResponse
    final case object Failure extends LoadBalancingCircuitBreakerBidiFlowTestResponse
  }
}

final class LoadBalancingCircuitBreakerBidiFlowTest extends AkkaTestKit
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with AkkaTestKitAutoShutDown {
  import LoadBalancingCircuitBreakerBidiFlowTest._

  private val testRequest = "test"
  private val createTestRequest = () => testRequest

  "LoadBalancingCircuitBreakerBidiFlow" should {
    "send canary requests at start if it is started in inactive state" in {
      val servers = ArraySeq("localhost:1111", "localhost:1112")
      val probes = runTestLoadBalandingCircuitBreakerBidiFlow(servers)
      val _ = probes.ensureSubscriptions()

      val _ = probes.responses.request(1)

      val requests = servers.map { server =>
        val request = probes.requestsWithDestination.requestNext()
        request.request mustEqual testRequest
        request.destination mustEqual server
        request
      }
      requests.foreach { request =>
        probes.responsesWithDestination.sendNext(createDestOkResponse(request))
      }

      probes.responses.expectNoMessage(50.millis)
    }

    "send canary requests at start before it sends regular requests if it is started in inactive state" in {
      val servers = ArraySeq("localhost:1111")
      val probes = runTestLoadBalandingCircuitBreakerBidiFlow(servers)
      val _ = probes.ensureSubscriptions()

      val _ = probes.responses.request(1)
      val _ = probes.requests.sendNext("1")

      val request1 = probes.requestsWithDestination.requestNext()
      request1.request mustEqual testRequest
      request1.destination mustEqual servers(0)

      probes.requestsWithDestination.expectNoMessage(50.millis)

      probes.responsesWithDestination.sendNext(createDestOkResponse(request1))

      val request2 = probes.requestsWithDestination.requestNext()
      request2.request mustEqual "1"
      request2.destination mustEqual servers(0)
      probes.responsesWithDestination.sendNext(createDestOkResponse(request2))

      probes.responses.requestNext() mustEqual LoadBalancingCircuitBreakerBidiFlowTestResponse.Success
    }

    "respect isSuccessful mapping for recognizing successful requests" in {
      val servers = ArraySeq("localhost:1111")
      val isSuccessful: LoadBalancingCircuitBreakerBidiFlowTestResponse => Boolean = {
        case LoadBalancingCircuitBreakerBidiFlowTestResponse.Success => true
        case LoadBalancingCircuitBreakerBidiFlowTestResponse.Failure => true
        case LoadBalancingCircuitBreakerBidiFlowTestResponse.Timeout => false
      }
      val probes = runTestLoadBalandingCircuitBreakerBidiFlow(servers, isSuccessful = isSuccessful)
      val _ = probes.ensureSubscriptions()

      val _ = probes.responses.request(1)
      val _ = probes.requests.sendNext("1")

      val request1 = probes.requestsWithDestination.requestNext()
      request1.request mustEqual testRequest
      request1.destination mustEqual servers(0)
      probes.responsesWithDestination.sendNext(createDestErrorResponse(request1))

      val request2 = probes.requestsWithDestination.requestNext()
      request2.request mustEqual "1"
      request2.destination mustEqual servers(0)
      probes.responsesWithDestination.sendNext(createDestOkResponse(request2))

      probes.responses.requestNext() mustEqual LoadBalancingCircuitBreakerBidiFlowTestResponse.Success
    }

    "retry error responses for canary requests" in {
      val servers = ArraySeq("localhost:1111")
      val probes = runTestLoadBalandingCircuitBreakerBidiFlow(
        servers,
        resetTimeout = Timeout(100.millis)
      )
      val _ = probes.ensureSubscriptions()

      val _ = probes.responses.request(1)
      val _ = probes.requests.sendNext("1")

      for (i <- 1 to 4) {
        val request = probes.requestsWithDestination.requestNext()
        request.request mustEqual testRequest
        request.destination mustEqual servers(0)
        if (i < 4) {
          probes.responsesWithDestination.sendNext(createDestTimeoutResponse(request))
        }
        else {
          probes.responsesWithDestination.sendNext(createDestOkResponse(request))
        }
      }

      val request = probes.requestsWithDestination.requestNext()
      request.request mustEqual "1"
      request.destination mustEqual servers(0)
      probes.responsesWithDestination.sendNext(createDestOkResponse(request))

      probes.responses.requestNext() mustEqual LoadBalancingCircuitBreakerBidiFlowTestResponse.Success
    }

    "load balance requests" in {
      val servers = ArraySeq("localhost:1111", "localhost:1112", "localhost:1113")
      val serversIterator = Iterator.continually(servers.iterator).flatten
      val probes = runTestLoadBalandingCircuitBreakerBidiFlow(
        servers,
        initialServerStateInactive = false
      )
      val _ = probes.ensureSubscriptions()

      for (i <- 1 to 5) {
        val _ = probes.requests.sendNext(i.toString)
        val request = probes.requestsWithDestination.requestNext()
        request.request mustEqual i.toString
        request.destination mustEqual serversIterator.next()
        probes.responsesWithDestination.sendNext(createDestOkResponse(request))
        probes.responses.requestNext() mustEqual LoadBalancingCircuitBreakerBidiFlowTestResponse.Success
      }
    }

    "respect max failures and reset timeout parameters" in {
      val servers = ArraySeq("localhost:1111")
      val maxFailures: PosInt = 3
      val resetTimeout = Timeout(500.millis)
      val probes = runTestLoadBalandingCircuitBreakerBidiFlow(
        servers = servers,
        maxFailuresCount = maxFailures,
        resetTimeout = resetTimeout,
        initialServerStateInactive = false
      )
      val _ = probes.ensureSubscriptions()

      for (i <- 1 to maxFailures.value) {
        val _ = probes.requests.sendNext(i.toString)
        val request = probes.requestsWithDestination.requestNext()
        request.request mustEqual i.toString
        request.destination mustEqual servers(0)
        probes.responsesWithDestination.sendNext(createDestTimeoutResponse(request))
        probes.responses.requestNext() mustEqual LoadBalancingCircuitBreakerBidiFlowTestResponse.Timeout
      }

      val _ = probes.requests.sendNext("4")

      val additionalToleration = 25.millis
      // make sure canary request was not sent for resetTimeout duration
      probes.requestsWithDestination.request(1)
      probes.responses.request(1)
      probes.requestsWithDestination.expectNoMessage(resetTimeout.duration - additionalToleration)

      // now we expect canary request to be sent
      val canaryRequest = probes.requestsWithDestination.expectNext()
      canaryRequest.request mustEqual testRequest
      canaryRequest.destination mustEqual servers(0)
      probes.responsesWithDestination.sendNext(createDestOkResponse(canaryRequest))

      // now we expect regular request
      val request = probes.requestsWithDestination.requestNext()
      request.request mustEqual "4"
      request.destination mustEqual servers(0)
      probes.responsesWithDestination.sendNext(createDestOkResponse(request))
      probes.responses.expectNext() mustEqual LoadBalancingCircuitBreakerBidiFlowTestResponse.Success
    }

    "load balance requests among active servers" in {
      val servers = ArraySeq("localhost:1111", "localhost:1112", "localhost:1113")
      val maxFailures: PosInt = 1
      val resetTimeout = Timeout(500.millis)
      val probes = runTestLoadBalandingCircuitBreakerBidiFlow(
        servers = servers,
        maxFailuresCount = maxFailures,
        resetTimeout = resetTimeout,
        initialServerStateInactive = false
      )
      val _ = probes.ensureSubscriptions()

      for (i <- servers.indices) {
        val _ = probes.requests.sendNext(i.toString)
        val request = probes.requestsWithDestination.requestNext()
        request.request mustEqual i.toString
        request.destination mustEqual servers(i)
        if (i == 1) {
          probes.responsesWithDestination.sendNext(createDestTimeoutResponse(request))
          probes.responses.requestNext() mustEqual LoadBalancingCircuitBreakerBidiFlowTestResponse.Timeout
        }
        else {
          probes.responsesWithDestination.sendNext(createDestOkResponse(request))
          probes.responses.requestNext() mustEqual LoadBalancingCircuitBreakerBidiFlowTestResponse.Success
        }
      }

      var healthyServersIterator = Iterator.continually(Iterator(servers(0), servers(2))).flatten
      for (i <- 1 to 6) {
        val _ = probes.requests.sendNext(i.toString)
        val request = probes.requestsWithDestination.requestNext()
        request.request mustEqual i.toString
        request.destination mustEqual healthyServersIterator.next()
        probes.responsesWithDestination.sendNext(createDestOkResponse(request))
        probes.responses.requestNext() mustEqual LoadBalancingCircuitBreakerBidiFlowTestResponse.Success
      }

      // now let's wait for the canary request
      val _ = probes.requestsWithDestination.request(1)
      val _ = probes.responses.request(1)
      val canaryRequest = probes.requestsWithDestination.expectNext()
      canaryRequest.request mustEqual testRequest
      canaryRequest.destination mustEqual servers(1)
      probes.responsesWithDestination.sendNext(createDestOkResponse(canaryRequest))

      // all servers are healthy now
      healthyServersIterator = Iterator.continually(servers.iterator).flatten
      for (i <- 1 to 6) {
        val _ = probes.requests.sendNext(i.toString)
        val request = probes.requestsWithDestination.requestNext()
        request.request mustEqual i.toString
        request.destination mustEqual healthyServersIterator.next()
        probes.responsesWithDestination.sendNext(createDestOkResponse(request))
        probes.responses.requestNext() mustEqual LoadBalancingCircuitBreakerBidiFlowTestResponse.Success
      }
    }

  }

  private def createDestOkResponse(request: LoadBalancingCircuitBreakerRequest[String, String])
    : LoadBalancingCircuitBreakerResponse[LoadBalancingCircuitBreakerBidiFlowTestResponse, String] = {
    createDestResponse(request, LoadBalancingCircuitBreakerBidiFlowTestResponse.Success)
  }

  private def createDestTimeoutResponse[Ctx](request: LoadBalancingCircuitBreakerRequest[String, String]) = {
    createDestResponse(request, LoadBalancingCircuitBreakerBidiFlowTestResponse.Timeout)
  }

  private def createDestErrorResponse[Ctx](request: LoadBalancingCircuitBreakerRequest[String, String]) = {
    createDestResponse(request, LoadBalancingCircuitBreakerBidiFlowTestResponse.Failure)
  }

  private def createDestResponse(
    request: LoadBalancingCircuitBreakerRequest[String, String],
    resp: LoadBalancingCircuitBreakerBidiFlowTestResponse
  ): LoadBalancingCircuitBreakerResponse[LoadBalancingCircuitBreakerBidiFlowTestResponse, String] = {
    val now = Instant.now
    LoadBalancingCircuitBreakerResponse(
      response = resp,
      from = request.destination,
      startedAt = now.minusMillis(5),
      completedAt = now
    )
  }

  private def runTestLoadBalandingCircuitBreakerBidiFlow(
    servers: IndexedSeq[String],
    maxFailuresCount: PosInt = 5,
    resetTimeout: Timeout = Timeout(1.second),
    initialServerStateInactive: Boolean = true,
    isSuccessful: LoadBalancingCircuitBreakerBidiFlowTestResponse => Boolean = {
      case LoadBalancingCircuitBreakerBidiFlowTestResponse.Success => true
      case _ => false
    },
    onOneServerGotActiveChange: () => Unit = () => (),
    onAllServersInactiveChange: () => Unit = () => ()
  ): LoadBalancingCircuitBreakerBidiFlowTestProbes = {
    val loadBalancingCircuitBreakerFlow = {
      LoadBalancingCircuitBreakerBidiFlow[String, LoadBalancingCircuitBreakerBidiFlowTestResponse, String](
        config = LoadBalancingCircuitBreakerPerServerConfig(
          maxFailuresCount = maxFailuresCount,
          resetTimeout = resetTimeout,
          initialServerStateInactive = initialServerStateInactive
        ),
        servers = servers,
        clock = Clock.systemUTC(),
        createTestRequest = createTestRequest,
        isSuccessful = isSuccessful,
        onOneServerGotActiveChange = onOneServerGotActiveChange,
        onAllServersInactiveChange = onAllServersInactiveChange
      ).joinMat(
        Flow.fromSinkAndSourceMat(
          TestSink.probe[LoadBalancingCircuitBreakerRequest[String, String]],
          TestSource.probe[LoadBalancingCircuitBreakerResponse[LoadBalancingCircuitBreakerBidiFlowTestResponse, String]]
        )(Keep.both)
      )(Keep.right)
    }

    TestSource
      .probe[String]
      .viaMat(loadBalancingCircuitBreakerFlow)(Keep.both)
      .toMat(TestSink.probe[LoadBalancingCircuitBreakerBidiFlowTestResponse]) {
        case ((requests, (requestsWithDestination, responsesWithDestination)), responses) =>
          LoadBalancingCircuitBreakerBidiFlowTestProbes(
            requests = requests,
            requestsWithDestination,
            responsesWithDestination,
            responses = responses
          )
      }
      .run()
  }

}
