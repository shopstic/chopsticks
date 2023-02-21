/*
package dev.chopsticks.stream

import akka.NotUsed
import akka.stream.scaladsl.BidiFlow
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogicWithLogging}
import akka.util.Timeout
import eu.timepit.refined.types.numeric.PosInt
import pureconfig.ConfigConvert

import java.time.{Clock, Instant}
import java.util.concurrent.TimeUnit
import scala.annotation.nowarn
import scala.collection.immutable.ArraySeq
import scala.concurrent.duration.{Duration, FiniteDuration}

final case class LoadBalancingCircuitBreakerPerServerConfig(
  maxFailuresCount: PosInt,
  resetTimeout: Timeout,
  initialServerStateInactive: Boolean,
  resetFailuresOnSuccess: Boolean
)

object LoadBalancingCircuitBreakerPerServerConfig {
  import dev.chopsticks.util.config.PureconfigConverters._
  //noinspection TypeAnnotation
  implicit val configConvert = ConfigConvert[LoadBalancingCircuitBreakerPerServerConfig]
}

final case class LoadBalancingCircuitBreakerRequest[Request, ServerId](
  request: Request,
  destination: ServerId,
  isTestRequest: Boolean
)

final case class LoadBalancingCircuitBreakerResponse[Response, ServerId](
  response: Response,
  from: ServerId,
  startedAt: Instant,
  completedAt: Instant
)

object LoadBalancingCircuitBreakerBidiFlow {

  def apply[Request, Response, ServerId](
    config: LoadBalancingCircuitBreakerPerServerConfig,
    servers: IndexedSeq[ServerId],
    clock: Clock,
    createTestRequest: () => Request,
    isSuccessful: Response => Boolean,
    onServerTransitionToActive: ServerId => () => Unit = (_: ServerId) => () => (),
    onServerTransitionToInactive: ServerId => () => Unit = (_: ServerId) => () => (),
    onOneServerGotActiveChange: () => Unit = () => (),
    onAllServersInactiveChange: () => Unit = () => ()
  ): BidiFlow[
    Request,
    LoadBalancingCircuitBreakerRequest[Request, ServerId],
    LoadBalancingCircuitBreakerResponse[Response, ServerId],
    Response,
    NotUsed
  ] = {
    BidiFlow.fromGraph(
      new LoadBalancingCircuitBreakerBidiFlow[Request, Response, ServerId](
        config,
        servers,
        clock,
        createTestRequest,
        isSuccessful,
        onServerTransitionToActive,
        onServerTransitionToInactive,
        onOneServerGotActiveChange,
        onAllServersInactiveChange
      )
    )
  }

  sealed abstract private[LoadBalancingCircuitBreakerBidiFlow] class LoadBalancingCircuitBreakerRequestType
  final private[LoadBalancingCircuitBreakerBidiFlow] case object LoadBalancingCircuitBreakerCanaryRequest
      extends LoadBalancingCircuitBreakerRequestType
  final private[LoadBalancingCircuitBreakerBidiFlow] case object LoadBalancingCircuitBreakerRegularRequest
      extends LoadBalancingCircuitBreakerRequestType

  private[LoadBalancingCircuitBreakerBidiFlow] object LoadBalancingCircuitBreakerServerState {
    val Active = 0
    val HalfOpenWaitingForAssignment = 1
    val HalfOpenWaitingForResponse = 2
    val Inactive = 3
  }

  private[LoadBalancingCircuitBreakerBidiFlow] class LoadBalancingCircuitBreakerServerState[ServerId](
    val serverId: ServerId,
    initTime: Instant,
    config: LoadBalancingCircuitBreakerPerServerConfig,
    onServerTransitionToActive: () => Unit,
    onServerTransitionToInactive: () => Unit
  ) {
    import LoadBalancingCircuitBreakerServerState._

    private var state = Active
    private var pendingRequests = 0L
    private var transitionedToInactiveAt = Option.empty[Instant]
    private val failureTimestamps = scala.collection.mutable.Queue.empty[Instant]

    if (config.initialServerStateInactive) {
      transitionToInactive(initTime)
      // transition to half open right away
      transitionToHalfOpenWaitingForAssignment()
    }
    else {
      transitionToActive()
    }

    def startRequest(): Unit = {
      if (isHalfOpenWaitingForAssignment) {
        transitionToHalfOpenWaitingForResponse()
      }
      pendingRequests += 1L
    }

    def registerResponse(
      isSuccessful: Boolean,
      responseTimestamp: Instant
    ): LoadBalancingCircuitBreakerRequestType = {
      pendingRequests -= 1L
      if (isHalfOpenWaitingForResponse) {
        if (isSuccessful) transitionToActive()
        else transitionToInactive(responseTimestamp)
        LoadBalancingCircuitBreakerCanaryRequest
      }
      else {
        if (!isSuccessful) {
          dropExpiredFailures(responseTimestamp)
          val _ = failureTimestamps.enqueue(responseTimestamp)
          if (config.maxFailuresCount.value <= failureTimestamps.length) {
            transitionToInactive(responseTimestamp)
          }
        }
        else if (config.resetFailuresOnSuccess) {
          if (isActive) failureTimestamps.clear()
          else transitionToActive()
        }
        LoadBalancingCircuitBreakerRegularRequest
      }
    }

    def dropExpiredFailures(now: Instant): Unit = {
      while (
        failureTimestamps.nonEmpty &&
        failureTimestamps.front.plusMillis(config.resetTimeout.duration.toMillis).isBefore(now)
      ) {
        val _ = failureTimestamps.dequeue()
      }
    }

    // returns true if state transition succeeded, false otherwise
    def tryTransitionToHalfOpen(now: Instant): Boolean = {
      val canTransition =
        isInactive &&
          pendingRequests == 0L &&
          transitionedToInactiveAt.get.plusMillis(config.resetTimeout.duration.toMillis).isBefore(now)
      if (canTransition) {
        transitionToHalfOpenWaitingForAssignment()
      }
      canTransition
    }

    def isActive: Boolean = state == Active
    def isHalfOpenWaitingForAssignment: Boolean =
      state == HalfOpenWaitingForAssignment
    def isHalfOpenWaitingForResponse: Boolean =
      state == HalfOpenWaitingForResponse
    def isHalfOpen: Boolean =
      isHalfOpenWaitingForAssignment || isHalfOpenWaitingForResponse
    def isInactive: Boolean =
      state == Inactive

    private def transitionToActive(): Unit = {
      onServerTransitionToActive()
      state = Active
      transitionedToInactiveAt = None
      failureTimestamps.clear()
    }

    private def transitionToHalfOpenWaitingForAssignment(): Unit = {
      state = HalfOpenWaitingForAssignment
      transitionedToInactiveAt = None
      failureTimestamps.clear()
    }

    private def transitionToHalfOpenWaitingForResponse(): Unit = {
      state = HalfOpenWaitingForResponse
      transitionedToInactiveAt = None
      failureTimestamps.clear()
    }

    private def transitionToInactive(now: Instant): Unit = {
      onServerTransitionToInactive()
      state = Inactive
      transitionedToInactiveAt = Some(now)
      failureTimestamps.clear()
    }
  }

  private[LoadBalancingCircuitBreakerBidiFlow] class LoadBalancingCircuitBreakerLogic[ServerId](
    config: LoadBalancingCircuitBreakerPerServerConfig,
    allServers: IndexedSeq[ServerId],
    clock: Clock,
    onServerTransitionToActive: ServerId => () => Unit = (_: ServerId) => () => (),
    onServerTransitionToInactive: ServerId => () => Unit = (_: ServerId) => () => (),
    onOneServerGotActiveChange: () => Unit = () => (),
    onAllServersInactiveChange: () => Unit = () => ()
  ) {

    private type ServerState = LoadBalancingCircuitBreakerServerState[ServerId]

    private val servers: ArraySeq[ServerState] = {
      val initTime = clock.instant()
      allServers.iterator
        .map { s =>
          new LoadBalancingCircuitBreakerServerState[ServerId](
            serverId = s,
            initTime = initTime,
            config = config,
            onServerTransitionToActive = onServerTransitionToActive(s),
            onServerTransitionToInactive = onServerTransitionToInactive(s)
          )
        }
        .to(ArraySeq)
    }

    private var nextServerIndex = 0
    private var activeServerExists = countActiveServers > 0

    // this method is stateful, when a server is returned, the BidiFlow must use that value to send request to this destination
    def startRequest(): Option[ServerId] = {
      val result = pickNextDestination()
      result.foreach(_.startRequest())
      result.map(_.serverId)
    }

    // this method is stateful, when a server is returned, the BidiFlow must use that value to send request to this destination
    def startCanaryRequest(): Option[ServerId] = {
      servers.find(_.isHalfOpenWaitingForAssignment) match {
        case Some(server) =>
          server.startRequest()
          Some(server.serverId)
        case None =>
          None
      }
    }

    def registerResponse(
      server: ServerId,
      isSuccessful: Boolean,
      responseTimestamp: Instant
    ): LoadBalancingCircuitBreakerRequestType = {
      val stateBefore = hasActiveServers
      val result = getServerState(server).registerResponse(isSuccessful, responseTimestamp)
      val stateAfter = countActiveServers > 0
      if (stateBefore != stateAfter && stateAfter) {
        activeServerExists = true
        onOneServerGotActiveChange()
      }
      else if (stateBefore != stateAfter && !stateAfter) {
        activeServerExists = false
        onAllServersInactiveChange()
      }
      result
    }

    def onTimer(now: Instant): Unit = {
      servers.foreach { server =>
        val _ = server.tryTransitionToHalfOpen(now)
      }
    }

    def activeServers: IndexedSeq[ServerId] = servers.collect { case s if s.isActive => s.serverId }
    def inactiveServers: IndexedSeq[ServerId] = servers.collect { case s if s.isInactive => s.serverId }
    def halfOpenServers: IndexedSeq[ServerId] = servers.collect { case s if s.isHalfOpen => s.serverId }
    def halfOpenWaitingForAssignmentServers: IndexedSeq[ServerId] =
      servers.collect { case s if s.isHalfOpenWaitingForAssignment => s.serverId }
    def halfOpenWaitingForResponseServers: IndexedSeq[ServerId] =
      servers.collect { case s if s.isHalfOpenWaitingForResponse => s.serverId }

    def hasActiveServers: Boolean = activeServerExists
    def countActiveServers: Int = servers.count(_.isActive)

    private def getServerState(server: ServerId): ServerState = {
      var i = 0
      while (i < servers.length) {
        val current = servers(i)
        if (current.serverId == server) {
          return current
        }
        i += 1
      }
      throw new RuntimeException(
        s"Got incorrect server: $server. Available servers are: ${servers.map(_.serverId).mkString(", ")}"
      )
    }

    private def pickNextDestination(): Option[ServerState] = {
      if (!hasActiveServers) None
      else {
        // currently we route only in round robin
        var next = servers(nextServerIndex % servers.length)
        nextServerIndex += 1
        while (!next.isActive) {
          next = servers(nextServerIndex % servers.length)
          nextServerIndex += 1
        }
        // make sure nextServerIndex doesn't exceed Int.MaxValue
        nextServerIndex = nextServerIndex % servers.length
        Some(next)
      }
    }
  }

  final private case object LoadBalancingCircuitBreakerBidiFlowTimer
}

final class LoadBalancingCircuitBreakerBidiFlow[Request, Response, ServerId](
  config: LoadBalancingCircuitBreakerPerServerConfig,
  servers: IndexedSeq[ServerId],
  clock: Clock,
  createTestRequest: () => Request,
  isSuccessful: Response => Boolean,
  onServerTransitionToActive: ServerId => () => Unit = (_: ServerId) => () => (),
  onServerTransitionToInactive: ServerId => () => Unit = (_: ServerId) => () => (),
  onOneServerGotActiveChange: () => Unit = () => (),
  onAllServersInactiveChange: () => Unit = () => ()
) extends GraphStage[BidiShape[
      Request,
      LoadBalancingCircuitBreakerRequest[Request, ServerId],
      LoadBalancingCircuitBreakerResponse[Response, ServerId],
      Response
    ]] { bidiflow =>

  assert(servers.nonEmpty, "There are no servers configured for LoadBalancingCircuitBreakerBidiFlow")

  private type RequestWithDest = LoadBalancingCircuitBreakerRequest[Request, ServerId]
  private type ResponseWithDest = LoadBalancingCircuitBreakerResponse[Response, ServerId]

  private val stageName = "LoadBalancingCircuitBreakerBidiFlow"

  private val requests = Inlet[Request](s"$stageName.requests.in")
  private val requestsWithDestination = Outlet[RequestWithDest](s"$stageName.requestsWithDest.out")
  private val responsesWithDestination = Inlet[ResponseWithDest](s"$stageName.responsesWithDest.in")
  private val responses = Outlet[Response](s"$stageName.responses.out")

  val shape: BidiShape[Request, RequestWithDest, ResponseWithDest, Response] =
    BidiShape.of(requests, requestsWithDestination, responsesWithDestination, responses)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new TimerGraphStageLogicWithLogging(shape) {
      import LoadBalancingCircuitBreakerBidiFlow._

      val state = new LoadBalancingCircuitBreakerLogic[ServerId](
        config = config,
        allServers = servers,
        clock = clock,
        onServerTransitionToActive = onServerTransitionToActive,
        onServerTransitionToInactive = onServerTransitionToInactive,
        onOneServerGotActiveChange = onOneServerGotActiveChange,
        onAllServersInactiveChange = onAllServersInactiveChange
      )
      var pendingRequest = Option.empty[Request]

      override def preStart(): Unit = {
        if (config.initialServerStateInactive) {
          onAllServersInactiveChange()
        }
        else {
          onOneServerGotActiveChange()
        }
        val intervalMs = Math.min(500, Math.max(20, config.resetTimeout.duration.toMillis / 3))
        val interval = FiniteDuration(intervalMs, TimeUnit.MILLISECONDS)
        scheduleAtFixedRate(
          timerKey = LoadBalancingCircuitBreakerBidiFlowTimer,
          initialDelay = Duration.Zero,
          interval = interval
        )
      }

      setHandler(
        requests,
        new InHandler {
          override def onPush(): Unit = {
            val request = grab(requests)
            if (isAvailable(requestsWithDestination)) {
              pendingRequest match {
                case Some(pending) =>
                  state.startRequest() match {
                    case Some(server) =>
                      pendingRequest = Some(request)
                      emit(
                        requestsWithDestination,
                        LoadBalancingCircuitBreakerRequest(pending, server, isTestRequest = false)
                      )
                    case None =>
                      failStage(
                        new IllegalStateException(
                          s"Unexpected state. No server is available and there is already a pending request. Configured servers: ${servers.mkString(", ")}"
                        )
                      )
                  }
                case None =>
                  state.startRequest() match {
                    case Some(server) =>
                      emit(
                        requestsWithDestination,
                        LoadBalancingCircuitBreakerRequest(request, server, isTestRequest = false)
                      )
                    case None =>
                      pendingRequest = Some(request)
                  }
              }
            }
            else {
              pendingRequest match {
                case Some(_) =>
                  failStage(new IllegalStateException(
                    s"$requests port is not available and there already exists pending request."
                  ))
                case None =>
                  pendingRequest = Some(request)
              }
            }
          }

          override def onUpstreamFinish(): Unit = {
            complete(requestsWithDestination)
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            fail(requestsWithDestination, ex)
          }
        }
      )

      setHandler(
        requestsWithDestination,
        new OutHandler {
          override def onPull(): Unit = {
            state.startCanaryRequest() match {
              case Some(server) =>
                emit(
                  requestsWithDestination,
                  LoadBalancingCircuitBreakerRequest(createTestRequest(), server, isTestRequest = true)
                )
              case None =>
                pendingRequest match {
                  case Some(request) =>
                    state.startRequest() match {
                      case Some(server) =>
                        pendingRequest = None
                        emit(
                          requestsWithDestination,
                          LoadBalancingCircuitBreakerRequest(request, server, isTestRequest = false)
                        )
                      case None =>
                        // no servers available, we need to wait
                        ()
                    }
                  case None =>
                    if (!hasBeenPulled(requests)) {
                      tryPull(requests)
                    }
                }
            }
          }

          override def onDownstreamFinish(cause: Throwable): Unit = {
            cancel(requests, cause)
          }
        }
      )

      setHandler(
        responsesWithDestination,
        new InHandler {
          override def onPush(): Unit = {
            val response = grab(responsesWithDestination)
            val success = isSuccessful(response.response)

            state.registerResponse(response.from, success, response.completedAt) match {
              case LoadBalancingCircuitBreakerCanaryRequest =>
                // drop canary requests
                tryPull(responsesWithDestination)
              case LoadBalancingCircuitBreakerRegularRequest =>
                emit(responses, response.response)
            }

            pendingRequest match {
              case Some(request) =>
                if (isAvailable(requestsWithDestination) && state.hasActiveServers) {
                  // there are active servers, so get is OK
                  val server = state.startRequest().get
                  pendingRequest = None
                  emit(
                    requestsWithDestination,
                    LoadBalancingCircuitBreakerRequest(request, server, isTestRequest = false)
                  )
                }
              case None =>
                if (!hasBeenPulled(requests) && isAvailable(requestsWithDestination) && state.hasActiveServers) {
                  tryPull(requests)
                }
            }
          }

          override def onUpstreamFinish(): Unit = {
            complete(responses)
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            fail(responses, ex)
          }
        }
      )

      setHandler(
        responses,
        new OutHandler {
          override def onPull(): Unit = {
            if (!hasBeenPulled(responsesWithDestination)) {
              tryPull(responsesWithDestination)
            }
          }

          override def onDownstreamFinish(cause: Throwable): Unit = {
            cancel(responsesWithDestination, cause)
          }
        }
      )

      override protected def onTimer(timerKey: Any): Unit = {
        timerKey match {
          case LoadBalancingCircuitBreakerBidiFlowTimer =>
            state.onTimer(clock.instant)
            if (isAvailable(requestsWithDestination) && (!hasBeenPulled(requests) || pendingRequest.isEmpty)) {
              state.startCanaryRequest() match {
                case Some(server) =>
                  emit(
                    requestsWithDestination,
                    LoadBalancingCircuitBreakerRequest(createTestRequest(), server, isTestRequest = true)
                  )
                case None =>
                  ()
              }
            }
          case _ =>
            ()
        }
      }

      @nowarn
      private def dumpState(): Unit = {
        log.debug(s"LoadBalancingCircuitBreaker state: pendingRequest={}, {}", pendingRequest.isDefined, serversInfo())
      }

      private def serversInfo() = {
        s"active=[${state.activeServers.mkString(", ")}], " +
          s"inactive=[${state.inactiveServers.mkString(", ")}], " +
          s"halfOpenWaitingForAssignmentServers=[${state.halfOpenWaitingForAssignmentServers.mkString(", ")}], " +
          s"halfOpenWaitingForResponseServers=[${state.halfOpenWaitingForResponseServers.mkString(", ")}]"
      }
    }
  }
}
 */
