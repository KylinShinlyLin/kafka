/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.controller

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.yammer.metrics.core.{Gauge, Timer}
import kafka.api._
import kafka.cluster.Broker
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{KafkaException, Node, Reconfigurable, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.collection.{Seq, Set, mutable}

object ControllerChannelManager {
  val QueueSizeMetricName = "QueueSize"
  val RequestRateAndQueueTimeMetricName = "RequestRateAndQueueTimeMs"
}

/**
  * 这个类的主要任务：
  * 1、管理 Controller 与集群 Broker 之间的连接，并为每个 Broker 创建 RequestSendThread 线程实例
  * 2、将要发送的请求放入到指定 Broker 的阻塞队列中，等待该 Broker 专属的 RequestSendThread 线程进行处理
  *
  * @param controllerContext
  * @param config
  * @param time
  * @param metrics
  * @param stateChangeLogger
  * @param threadNamePrefix
  */
class ControllerChannelManager(controllerContext: ControllerContext,
                               config: KafkaConfig,
                               time: Time,
                               metrics: Metrics,
                               stateChangeLogger: StateChangeLogger,
                               threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {

  import ControllerChannelManager._

  //这是一个 HashMap 类型，Key 是 Integer 类型，其实就是集群中 Broker 的 ID 信息，而 Value 是一个 ControllerBrokerStateInfo
  //ControllerBrokerStateInfo ,保存了Broker的一些状态
  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  newGauge("TotalQueueSize",
    () => brokerLock synchronized {
      brokerStateInfo.values.iterator.map(_.messageQueue.size).sum
    }
  )

  /**
    * Controller 组件在启动时，会调用 ControllerChannelManager 的 startup 方法。
    * 该方法会从元数据信息中找到集群的 Broker 列表，然后依次为它们调用 addBroker 方法，
    * 把它们加到 brokerStateInfo 变量中，最后再依次启动 brokerStateInfo 中的 RequestSendThread 线程
    */
  def startup() = {
    controllerContext.liveOrShuttingDownBrokers.foreach(addNewBroker)

    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  /**
    * 关闭所有 RequestSendThread 线程，并清空必要的资源
    */
  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.values.toList.foreach(removeExistingBroker)
    }
  }

  /**
    * 从名字看，就是发送请求，实际上就是把请求对象提交到请求队列
    *
    * @param brokerId
    * @param request
    * @param callback
    */
  def sendRequest(brokerId: Int, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(request.apiKey, request, callback, time.milliseconds()))
        case None =>
          warn(s"Not sending request $request to broker $brokerId, since it is offline.")
      }
    }
  }

  /**
    * 添加目标 Broker 到 brokerStateInfo 数据结构中，并创建必要的配套资源，如请求队列、RequestSendThread 线程对象等。
    * 最后，RequestSendThread 启动线程
    *
    * @param broker
    */
  def addBroker(broker: Broker): Unit = {
    // be careful here. Maybe the startup() API has already started the request send thread
    // 如果该Broker是新Broker的话
    brokerLock synchronized {
      if (!brokerStateInfo.contains(broker.id)) {
        // 将新Broker加入到Controller管理，并创建对应的RequestSendThread线程
        addNewBroker(broker)
        // 启动RequestSendThread线程
        startRequestSendThread(broker.id)
      }
    }
  }

  /**
    * 从 brokerStateInfo 移除目标 Broker 的相关数据
    *
    * @param brokerId
    */
  def removeBroker(brokerId: Int): Unit = {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  private def addNewBroker(broker: Broker): Unit = {
    // 为该Broker构造请求阻塞队列
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug(s"Controller ${config.brokerId} trying to connect to broker ${broker.id}")
    val controllerToBrokerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    val controllerToBrokerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)
    // 获取待连接Broker节点对象信息
    val brokerNode = broker.node(controllerToBrokerListenerName)
    val logContext = new LogContext(s"[Controller id=${config.brokerId}, targetBrokerId=${brokerNode.idString}] ")
    val (networkClient, reconfigurableChannelBuilder) = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        controllerToBrokerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        controllerToBrokerListenerName,
        config.saslMechanismInterBrokerProtocol,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )
      val reconfigurableChannelBuilder = channelBuilder match {
        case reconfigurable: Reconfigurable =>
          config.addReconfigurable(reconfigurable)
          Some(reconfigurable)
        case _ => None
      }
      // 创建NIO Selector实例用于网络数据传输
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> brokerNode.idString).asJava,
        false,
        channelBuilder,
        logContext
      )
      // 创建NetworkClient实例
      // NetworkClient类是Kafka clients工程封装的顶层网络客户端API
      // 提供了丰富的方法实现网络层IO数据传输
      val networkClient = new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        ClientDnsLookup.DEFAULT,
        time,
        false,
        new ApiVersions,
        logContext
      )
      (networkClient, reconfigurableChannelBuilder)
    }
    // 为这个RequestSendThread线程设置线程名称
    val threadName = threadNamePrefix match {
      case None => s"Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
      case Some(name) => s"$name:Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
    }
    // 构造请求处理速率监控指标
    val requestRateAndQueueTimeMetrics = newTimer(
      RequestRateAndQueueTimeMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS, brokerMetricTags(broker.id)
    )
    // 创建RequestSendThread实例
    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, requestRateAndQueueTimeMetrics, stateChangeLogger, threadName)
    requestThread.setDaemon(false)

    val queueSizeGauge = newGauge(QueueSizeMetricName, () => messageQueue.size, brokerMetricTags(broker.id))
    // 创建该Broker专属的ControllerBrokerStateInfo实例
    // 并将其加入到brokerStateInfo统一管理
    brokerStateInfo.put(broker.id, ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue,
      requestThread, queueSizeGauge, requestRateAndQueueTimeMetrics, reconfigurableChannelBuilder))
  }

  private def brokerMetricTags(brokerId: Int) = Map("broker-id" -> brokerId.toString)

  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo): Unit = {
    try {
      // Shutdown the RequestSendThread before closing the NetworkClient to avoid the concurrent use of the
      // non-threadsafe classes as described in KAFKA-4959.
      // The call to shutdownLatch.await() in ShutdownableThread.shutdown() serves as a synchronization barrier that
      // hands off the NetworkClient from the RequestSendThread to the ZkEventThread.
      brokerState.reconfigurableChannelBuilder.foreach(config.removeReconfigurable)
      brokerState.requestSendThread.shutdown()
      brokerState.networkClient.close()
      brokerState.messageQueue.clear()
      removeMetric(QueueSizeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      removeMetric(RequestRateAndQueueTimeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  protected def startRequestSendThread(brokerId: Int): Unit = {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if (requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

/**
  * controller发送请求到队列中的数据封装模型
  *
  * @param apiKey
  * @param request
  * @param callback
  * @param enqueueTimeMs
  */
case class QueueItem(apiKey: ApiKeys, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                     callback: AbstractResponse => Unit, enqueueTimeMs: Long)

/**
  * 用于发送controller请求的线程
  *
  * @param controllerId      Controller所在Broker的Id
  * @param controllerContext Controller元数据信息
  * @param queue             请求阻塞队列
  * @param networkClient     用于执行发送的网络I/O类
  * @param brokerNode        目标Broker节点
  * @param config            Kafka配置信息
  * @param time
  * @param requestRateAndQueueTimeMetrics
  * @param stateChangeLogger
  * @param name
  */
class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val queue: BlockingQueue[QueueItem],
                        val networkClient: NetworkClient,
                        val brokerNode: Node,
                        val config: KafkaConfig,
                        val time: Time,
                        val requestRateAndQueueTimeMetrics: Timer,
                        val stateChangeLogger: StateChangeLogger,
                        name: String)
  extends ShutdownableThread(name = name) {

  logIdent = s"[RequestSendThread controllerId=$controllerId] "

  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  //核心执行代码
  override def doWork(): Unit = {

    def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)

    // 以阻塞的方式从阻塞队列中取出请求
    val QueueItem(apiKey, requestBuilder, callback, enqueueTimeMs) = queue.take()
    // 更新统计信息
    requestRateAndQueueTimeMetrics.update(time.milliseconds() - enqueueTimeMs, TimeUnit.MILLISECONDS)

    var clientResponse: ClientResponse = null
    try {
      var isSendSuccessful = false
      //发送出去后会等待respond，这里会进行一个循环
      while (isRunning && !isSendSuccessful) {
        // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
        // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
        try {
          // 如果没有创建与目标Broker的TCP连接，或连接暂时不可用
          if (!brokerReady()) {
            isSendSuccessful = false
            // 等待重试
            backoff()
          }
          else {
            val clientRequest = networkClient.newClientRequest(brokerNode.idString, requestBuilder,
              time.milliseconds(), true)
            // 发送请求，等待接收Response
            clientResponse = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
            isSendSuccessful = true
          }
        } catch {
          case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
            warn(s"Controller $controllerId epoch ${controllerContext.epoch} fails to send request $requestBuilder " +
              s"to broker $brokerNode. Reconnecting to broker.", e)
            networkClient.close(brokerNode.idString)
            isSendSuccessful = false
            backoff()
        }
      }
      // 如果接收到了Response
      if (clientResponse != null) {
        val requestHeader = clientResponse.requestHeader
        val api = requestHeader.apiKey
        // 此Response的请求类型必须是LeaderAndIsrRequest、StopReplicaRequest或UpdateMetadataRequest中的一种
        if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA)
          throw new KafkaException(s"Unexpected apiKey received: $apiKey")

        val response = clientResponse.responseBody

        stateChangeLogger.withControllerEpoch(controllerContext.epoch).trace(s"Received response " +
          s"${response.toString(requestHeader.apiVersion)} for request $api with correlation id " +
          s"${requestHeader.correlationId} sent to broker $brokerNode")

        if (callback != null) {
          callback(response) // 处理回调
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Controller $controllerId fails to send a request to broker $brokerNode", e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
        networkClient.close(brokerNode.idString)
    }
  }

  private def brokerReady(): Boolean = {
    try {
      if (!NetworkClientUtils.isReady(networkClient, brokerNode, time.milliseconds())) {
        if (!NetworkClientUtils.awaitReady(networkClient, brokerNode, time, socketTimeoutMs))
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info(s"Controller $controllerId connected to $brokerNode for sending state change requests")
      }

      true
    } catch {
      case e: Throwable =>
        warn(s"Controller $controllerId's connection to broker $brokerNode was unsuccessful", e)
        networkClient.close(brokerNode.idString)
        false
    }
  }

  override def initiateShutdown(): Boolean = {
    if (super.initiateShutdown()) {
      networkClient.initiateClose()
      true
    } else
      false
  }
}

class ControllerBrokerRequestBatch(config: KafkaConfig,
                                   controllerChannelManager: ControllerChannelManager,
                                   controllerEventManager: ControllerEventManager,
                                   controllerContext: ControllerContext,
                                   stateChangeLogger: StateChangeLogger)
  extends AbstractControllerBrokerRequestBatch(config, controllerContext, stateChangeLogger) {

  def sendEvent(event: ControllerEvent): Unit = {
    controllerEventManager.put(event)
  }

  def sendRequest(brokerId: Int,
                  request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    controllerChannelManager.sendRequest(brokerId, request, callback)
  }

}

case class StopReplicaRequestInfo(replica: PartitionAndReplica, deletePartition: Boolean)

abstract class AbstractControllerBrokerRequestBatch(config: KafkaConfig,
                                                    controllerContext: ControllerContext,
                                                    stateChangeLogger: StateChangeLogger) extends Logging {
  val controllerId: Int = config.brokerId
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, LeaderAndIsrPartitionState]]
  val stopReplicaRequestMap = mutable.Map.empty[Int, ListBuffer[StopReplicaRequestInfo]]
  val updateMetadataRequestBrokerSet = mutable.Set.empty[Int]
  val updateMetadataRequestPartitionInfoMap = mutable.Map.empty[TopicPartition, UpdateMetadataPartitionState]

  def sendEvent(event: ControllerEvent): Unit

  def sendRequest(brokerId: Int,
                  request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit

  def newBatch(): Unit = {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        s"a new one. Some LeaderAndIsr state changes $leaderAndIsrRequestMap might be lost ")
    if (stopReplicaRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some StopReplica state changes $stopReplicaRequestMap might be lost ")
    if (updateMetadataRequestBrokerSet.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some UpdateMetadata state changes to brokers $updateMetadataRequestBrokerSet with partition info " +
        s"$updateMetadataRequestPartitionInfoMap might be lost ")
  }

  def clear(): Unit = {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }

  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int],
                                       topicPartition: TopicPartition,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicaAssignment: ReplicaAssignment,
                                       isNew: Boolean): Unit = {

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyNew = result.get(topicPartition).exists(_.isNew)
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      result.put(topicPartition, new LeaderAndIsrPartitionState()
        .setTopicName(topicPartition.topic)
        .setPartitionIndex(topicPartition.partition)
        .setControllerEpoch(leaderIsrAndControllerEpoch.controllerEpoch)
        .setLeader(leaderAndIsr.leader)
        .setLeaderEpoch(leaderAndIsr.leaderEpoch)
        .setIsr(leaderAndIsr.isr.map(Integer.valueOf).asJava)
        .setZkVersion(leaderAndIsr.zkVersion)
        .setReplicas(replicaAssignment.replicas.map(Integer.valueOf).asJava)
        .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
        .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
        .setIsNew(isNew || alreadyNew))
    }

    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
  }

  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int],
                                      topicPartition: TopicPartition,
                                      deletePartition: Boolean): Unit = {
    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val stopReplicaInfos = stopReplicaRequestMap.getOrElseUpdate(brokerId, ListBuffer.empty[StopReplicaRequestInfo])
      stopReplicaInfos.append(StopReplicaRequestInfo(PartitionAndReplica(topicPartition, brokerId), deletePartition))
    }
  }

  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicPartition]): Unit = {

    def updateMetadataRequestPartitionInfo(partition: TopicPartition, beingDeleted: Boolean): Unit = {
      controllerContext.partitionLeadershipInfo.get(partition) match {
        case Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)) =>
          val replicas = controllerContext.partitionReplicaAssignment(partition)
          val offlineReplicas = replicas.filter(!controllerContext.isReplicaOnline(_, partition))
          val updatedLeaderAndIsr =
            if (beingDeleted) LeaderAndIsr.duringDelete(leaderAndIsr.isr)
            else leaderAndIsr

          val partitionStateInfo = new UpdateMetadataPartitionState()
            .setTopicName(partition.topic)
            .setPartitionIndex(partition.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(updatedLeaderAndIsr.leader)
            .setLeaderEpoch(updatedLeaderAndIsr.leaderEpoch)
            .setIsr(updatedLeaderAndIsr.isr.map(Integer.valueOf).asJava)
            .setZkVersion(updatedLeaderAndIsr.zkVersion)
            .setReplicas(replicas.map(Integer.valueOf).asJava)
            .setOfflineReplicas(offlineReplicas.map(Integer.valueOf).asJava)
          updateMetadataRequestPartitionInfoMap.put(partition, partitionStateInfo)

        case None =>
          info(s"Leader not yet assigned for partition $partition. Skip sending UpdateMetadataRequest.")
      }
    }

    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
    partitions.foreach(partition => updateMetadataRequestPartitionInfo(partition,
      beingDeleted = controllerContext.topicsToBeDeleted.contains(partition.topic)))
  }

  private def sendLeaderAndIsrRequest(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    val leaderAndIsrRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 4
      else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV0) 3
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_1_0_IV0) 1
      else 0

    leaderAndIsrRequestMap.filterKeys(controllerContext.liveOrShuttingDownBrokerIds.contains).foreach {
      case (broker, leaderAndIsrPartitionStates) =>
        if (stateChangeLog.isTraceEnabled) {
          leaderAndIsrPartitionStates.foreach { case (topicPartition, state) =>
            val typeOfRequest =
              if (broker == state.leader) "become-leader"
              else "become-follower"
            stateChangeLog.trace(s"Sending $typeOfRequest LeaderAndIsr request $state to broker $broker for partition $topicPartition")
          }
        }
        val leaderIds = leaderAndIsrPartitionStates.map(_._2.leader).toSet
        val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.node(config.interBrokerListenerName)
        }
        val brokerEpoch = controllerContext.liveBrokerIdAndEpochs(broker)
        val leaderAndIsrRequestBuilder = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId,
          controllerEpoch, brokerEpoch, leaderAndIsrPartitionStates.values.toBuffer.asJava, leaders.asJava)
        sendRequest(broker, leaderAndIsrRequestBuilder, (r: AbstractResponse) => {
          val leaderAndIsrResponse = r.asInstanceOf[LeaderAndIsrResponse]
          sendEvent(LeaderAndIsrResponseReceived(leaderAndIsrResponse, broker))
        })


    }
    leaderAndIsrRequestMap.clear()
  }

  private def sendUpdateMetadataRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    updateMetadataRequestPartitionInfoMap.foreach { case (tp, partitionState) =>
      stateChangeLog.trace(s"Sending UpdateMetadata request $partitionState to brokers $updateMetadataRequestBrokerSet " +
        s"for partition $tp")
    }

    val partitionStates = updateMetadataRequestPartitionInfoMap.values.toBuffer
    val updateMetadataRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 6
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 5
      else if (config.interBrokerProtocolVersion >= KAFKA_1_0_IV0) 4
      else if (config.interBrokerProtocolVersion >= KAFKA_0_10_2_IV0) 3
      else if (config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
      else 0

    val liveBrokers = controllerContext.liveOrShuttingDownBrokers.iterator.map { broker =>
      val endpoints = if (updateMetadataRequestVersion == 0) {
        // Version 0 of UpdateMetadataRequest only supports PLAINTEXT
        val securityProtocol = SecurityProtocol.PLAINTEXT
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val node = broker.node(listenerName)
        Seq(new UpdateMetadataEndpoint()
          .setHost(node.host)
          .setPort(node.port)
          .setSecurityProtocol(securityProtocol.id)
          .setListener(listenerName.value))
      } else {
        broker.endPoints.map { endpoint =>
          new UpdateMetadataEndpoint()
            .setHost(endpoint.host)
            .setPort(endpoint.port)
            .setSecurityProtocol(endpoint.securityProtocol.id)
            .setListener(endpoint.listenerName.value)
        }
      }
      new UpdateMetadataBroker()
        .setId(broker.id)
        .setEndpoints(endpoints.asJava)
        .setRack(broker.rack.orNull)
    }.toBuffer

    updateMetadataRequestBrokerSet.intersect(controllerContext.liveOrShuttingDownBrokerIds).foreach { broker =>
      val brokerEpoch = controllerContext.liveBrokerIdAndEpochs(broker)
      val updateMetadataRequestBuilder = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion,
        controllerId, controllerEpoch, brokerEpoch, partitionStates.asJava, liveBrokers.asJava)
      sendRequest(broker, updateMetadataRequestBuilder, (r: AbstractResponse) => {
        val updateMetadataResponse = r.asInstanceOf[UpdateMetadataResponse]
        sendEvent(UpdateMetadataResponseReceived(updateMetadataResponse, broker))
      })

    }
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }

  private def sendStopReplicaRequests(controllerEpoch: Int): Unit = {
    val stopReplicaRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 1
      else 0

    def stopReplicaPartitionDeleteResponseCallback(brokerId: Int)(response: AbstractResponse): Unit = {
      val stopReplicaResponse = response.asInstanceOf[StopReplicaResponse]
      val partitionErrorsForDeletingTopics = stopReplicaResponse.partitionErrors.asScala.iterator.filter { pe =>
        controllerContext.isTopicDeletionInProgress(pe.topicName)
      }.map(pe => new TopicPartition(pe.topicName, pe.partitionIndex) -> Errors.forCode(pe.errorCode)).toMap

      if (partitionErrorsForDeletingTopics.nonEmpty)
        sendEvent(TopicDeletionStopReplicaResponseReceived(brokerId, stopReplicaResponse.error, partitionErrorsForDeletingTopics))
    }

    def createStopReplicaRequest(brokerEpoch: Long, requests: Seq[StopReplicaRequestInfo], deletePartitions: Boolean): StopReplicaRequest.Builder = {
      val partitions = requests.map(_.replica.topicPartition).asJava
      new StopReplicaRequest.Builder(stopReplicaRequestVersion, controllerId, controllerEpoch,
        brokerEpoch, deletePartitions, partitions)
    }

    stopReplicaRequestMap.filterKeys(controllerContext.liveOrShuttingDownBrokerIds.contains).foreach { case (brokerId, replicaInfoList) =>
      val (stopReplicaWithDelete, stopReplicaWithoutDelete) = replicaInfoList.partition(r => r.deletePartition)
      val brokerEpoch = controllerContext.liveBrokerIdAndEpochs(brokerId)

      if (stopReplicaWithDelete.nonEmpty) {
        debug(s"The stop replica request (delete = true) sent to broker $brokerId is ${stopReplicaWithDelete.mkString(",")}")
        val stopReplicaRequest = createStopReplicaRequest(brokerEpoch, stopReplicaWithDelete, deletePartitions = true)
        val callback = stopReplicaPartitionDeleteResponseCallback(brokerId) _
        sendRequest(brokerId, stopReplicaRequest, callback)
      }

      if (stopReplicaWithoutDelete.nonEmpty) {
        debug(s"The stop replica request (delete = false) sent to broker $brokerId is ${stopReplicaWithoutDelete.mkString(",")}")
        val stopReplicaRequest = createStopReplicaRequest(brokerEpoch, stopReplicaWithoutDelete, deletePartitions = false)
        sendRequest(brokerId, stopReplicaRequest)
      }
    }
    stopReplicaRequestMap.clear()
  }

  def sendRequestsToBrokers(controllerEpoch: Int): Unit = {
    try {
      val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerEpoch)
      sendLeaderAndIsrRequest(controllerEpoch, stateChangeLog)
      sendUpdateMetadataRequests(controllerEpoch, stateChangeLog)
      sendStopReplicaRequests(controllerEpoch)
    } catch {
      case e: Throwable =>
        if (leaderAndIsrRequestMap.nonEmpty) {
          error("Haven't been able to send leader and isr requests, current state of " +
            s"the map is $leaderAndIsrRequestMap. Exception message: $e")
        }
        if (updateMetadataRequestBrokerSet.nonEmpty) {
          error(s"Haven't been able to send metadata update requests to brokers $updateMetadataRequestBrokerSet, " +
            s"current state of the partition info is $updateMetadataRequestPartitionInfoMap. Exception message: $e")
        }
        if (stopReplicaRequestMap.nonEmpty) {
          error("Haven't been able to send stop replica requests, current state of " +
            s"the map is $stopReplicaRequestMap. Exception message: $e")
        }
        throw new IllegalStateException(e)
    }
  }
}

/**
  *
  * @param networkClient
  * @param brokerNode        目标 Broker 节点对象，里面封装了目标 Broker 的连接信息，比如主机名、端口号等
  * @param messageQueue      请求消息阻塞队列。你可以发现，Controller 为每个目标 Broker 都创建了一个消息队列
  * @param requestSendThread Controller 使用这个线程给目标 Broker 发送请求
  * @param queueSizeGauge
  * @param requestRateAndTimeMetrics
  * @param reconfigurableChannelBuilder
  */
case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node,
                                     messageQueue: BlockingQueue[QueueItem],
                                     requestSendThread: RequestSendThread,
                                     queueSizeGauge: Gauge[Int],
                                     requestRateAndTimeMetrics: Timer,
                                     reconfigurableChannelBuilder: Option[Reconfigurable])

