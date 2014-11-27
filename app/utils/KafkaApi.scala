package utils

import kafka.api.{OffsetFetchRequest, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import play.api.Play
import play.api.Play.current
import play.api.libs.json.{JsObject, JsNumber, JsArray, Json}
import scala.collection.JavaConverters._

/**
 * Created by xhuang on 11/25/14.
 */
object KafkaApi {
  val zookeepers = Play.configuration.getString("capillary.zookeepers").getOrElse("localhost:2181")
  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot")
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val zkClient = CuratorFrameworkFactory.newClient(zookeepers, retryPolicy);
  zkClient.start();
  val consumers: scala.collection.mutable.Map[String, SimpleConsumer] = scala.collection.mutable.Map.empty[String, SimpleConsumer]

  def makePath(parts: Seq[Option[String]]): String = {
    parts.foldLeft("")({ (path, maybeP) => maybeP.map({ p => path + "/" + p}).getOrElse(path)}).replace("//", "/")
  }

  def getKafkaState(topic: String): TopicInfo = {
    val replicas = getReplicas(topic).map(p => (p.partitionId, p)).toMap
    // Fetch info for each partition, given the topic
    val kParts = zkClient.getChildren.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"))))
    // For each partition fetch the JSON state data to find the leader for each partition
    val partitionInfos = kParts.asScala.map({ kp =>
      val jsonState = zkClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic), Some("partitions"), Some(kp), Some("state"))))
      val state = Json.parse(jsonState)
      val leader = (state \ "leader").as[Long]
      val isr = (state \ "isr").asInstanceOf[JsArray].value.map(_.toString()).toList
      //exclude topic which has no leader
      if (leader != -1L) {
        // Knowing the leader's ID, fetch info about that host so we can contact it.
        val idJson = zkClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/ids"), Some(leader.toString))))
        val leaderState = Json.parse(idJson)
        val host = (leaderState \ "host").as[String]
        val port = (leaderState \ "port").as[Int]

        // Talk to the lead broker and get offset data!
        //      val ks = new SimpleConsumer(host, port, 1000000, 64 * 1024, "capillary")
        if (!consumers.contains(host)) {
          consumers.put(host, new SimpleConsumer(host, port, 1000000, 64 * 1024, "capillary"))
        }
        val ks = consumers.get(host).get
        val topicAndPartition = TopicAndPartition(topic, kp.toInt)
        val requestInfo = Map[TopicAndPartition, PartitionOffsetRequestInfo](
          topicAndPartition -> new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)
        )
        val request = new OffsetRequest(
          requestInfo = requestInfo, versionId = OffsetRequest.CurrentVersion, clientId = "capillary")
        val response = ks.getOffsetsBefore(request);
        if (response.hasError) {
          println("ERROR!")
        }
        val offset = response.partitionErrorAndOffsets.get(topicAndPartition).get.offsets(0)
        ks.close
        //        (kp.toInt, offset, host)
        PartitionInfo(topic, kp.toInt, offset, host, replicas.get(kp.toInt).get.replicas, isr, NoIncrement)
      } else
      //        (kp.toInt, -1L, "-1")
        PartitionInfo(topic, kp.toInt, -1L, "-1", replicas.get(kp.toInt).get.replicas, List.empty[String], NoIncrement)
    }).toList
    val totalOffset = partitionInfos.map(_.offset).sum
    TopicInfo(System.currentTimeMillis(), topic, partitionInfos, totalOffset, NoIncrement, false)
  }

  def getKafkaState(topics: List[String]): List[TopicInfo] = {
    import scala.concurrent._
    import ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val init = List.empty[Future[TopicInfo]]
    val statesFutures = topics.foldLeft(init) {
      (list, topic) => {
        Future {
          getKafkaState(topic)
        } :: list
      }
    }
    val topicStatesFuture = Future.sequence(statesFutures)
    Await.result(topicStatesFuture, 10 seconds)
  }

  def getReplicas(topic: String): Set[Replicas] = {
    val topicInfo = zkClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/topics"), Some(topic))))
    val state = (Json.parse(topicInfo) \ "partitions").asInstanceOf[JsObject]
    val replicas = state.fieldSet.map {
      case (partitionId, reps) => {
        val id = partitionId.toInt
        val repList = reps.asInstanceOf[JsArray].value.map(_.as[Int]).toList
        Replicas(id, repList)
      }
    }.toSet
    println(replicas)
    replicas
  }

  /**
   * get ids of all Kafka brokers
   * 获取Kafka集群的所有broker信息
   * @return
   */
  def getBrokers(): Set[KafkaBroker] = {
    val idsPath = makePath(Seq(kafkaZkRoot, Some("brokers/ids")))
    val ids = zkClient.getChildren.forPath(idsPath)
    ids.asScala.map { id =>
      val brokerInfo = zkClient.getData.forPath(makePath(Seq(kafkaZkRoot, Some("brokers/ids"), Some(id))))
      val brokerJson = Json.parse(brokerInfo)
      val host = (brokerJson \ "host").as[String]
      val port = (brokerJson \ "port").as[Int]
      val jmxPort = (brokerJson \ "jmx_port").as[Int]
      val timestamp = (brokerJson \ "timestamp").as[String]
      val version = (brokerJson \ "version").as[Int]
      KafkaBroker(id.toInt, jmxPort, timestamp, version, host, port)
    }.toSet
  }

  /**
   * get partition leader distribution for this Kafka cluster
   * @param topicInfos
   */
  def getPartitionDistribution(topicInfos: List[TopicInfo]): Map[KafkaBroker, Set[PartitionInfo]] = {
    val brokers = getBrokers()
    val partitions = topicInfos.flatten(_.partitions)
    val distribution: Map[String, List[PartitionInfo]] = partitions.groupBy(_.leader)
    brokers.map{broker =>
      (broker, distribution.get(broker.host).getOrElse(List.empty[PartitionInfo]).toSet)
    }.toMap
  }
}

case class KafkaBroker(id: Int, jmxPort: Int, timestamp: String, version: Int, host: String, port: Int)

case class TopicInfo(timestamp: Long, topicName: String, partitions: List[PartitionInfo]
                     , total: Long, increment: Increment, isActive: Boolean)

case class PartitionInfo(topicName: String, id: Int, offset: Long, leader: String
                         , reps: List[Int]
                         , isr: List[String]
                         , increment: Increment)

case class Replicas(partitionId: Int, replicas: List[Int])

/**
 *
 * @param interval 时间间隔，以秒为单位
 * @param inc offset的变化值
 */
case class Increment(interval: Int, inc: Long) {
  override def toString(): String = {
    inc + " / " + interval + "s"
  }
}

object NoIncrement extends Increment(-1, -1)