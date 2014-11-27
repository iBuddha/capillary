package models

import kafka.api.{OffsetFetchRequest,OffsetRequest,PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.retry.ExponentialBackoffRetry
import play.api.libs.json._
import play.api.Play
import play.api.Play.current
import scala.collection.JavaConverters._
import utils.{TopicInfo, PartitionInfo, Increment}

object ZkKafka {

  case class Delta(partition: Int, amount: Option[Long], current: Long, storm: Option[Long])
  case class Topology(name: String, spoutRoot: String, topic: String)

  def topoCompFn(t1: Topology, t2: Topology) = {
    (t1.name compareToIgnoreCase t2.name) < 0
  }

  val zookeepers = Play.configuration.getString("capillary.zookeepers").getOrElse("localhost:2181")
  val kafkaZkRoot = Play.configuration.getString("capillary.kafka.zkroot")
  val stormZkRoot = Play.configuration.getString("capillary.storm.zkroot")
  val isTrident = Play.configuration.getString("capillary.use.trident").getOrElse(false)

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val zkClient = CuratorFrameworkFactory.newClient(zookeepers, retryPolicy);
  zkClient.start();
  var _topics: Option[Map[String, TopicInfo]] = None //cache topic information

  def makePath(parts: Seq[Option[String]]): String = {
    parts.foldLeft("")({ (path, maybeP) => maybeP.map({ p => path + "/" + p }).getOrElse(path) }).replace("//","/")
  }

  def applyBase(path: Seq[Option[String]]): Seq[Option[String]] = {
    if(isTrident.equals("true")) path ++ Seq(Some("user")) else path
  }

  def getTopologies: Seq[Topology] = {
    zkClient.getChildren.forPath(makePath(Seq(stormZkRoot))).asScala.map({ r =>
      getSpoutTopology(r)
    }).sortWith(topoCompFn)
  }

  def getSpoutTopology(root: String): Topology = {
    // Fetch the spout root
    val s = zkClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root)))))
//    // Use the first partition's data to build up info about the topology
    val pathToData = makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(s.get(0))))
    val jsonState = new String(zkClient.getData.forPath(pathToData))
    val state = Json.parse(jsonState)
    val topic = (state \ "topic").as[String]
    val name = (state \ "topology" \ "name").as[String]
    Topology(name = name, topic = topic, spoutRoot = root)
  }

  def getSpoutState(root: String, topic: String): Map[Int, Long] = {
    // There is basically nothing for error checking in here.
    val s = zkClient.getChildren.forPath(makePath(applyBase(Seq(stormZkRoot, Some(root)))))
    s.asScala.map({ pts =>
      val pathToData = makePath(applyBase(Seq(stormZkRoot, Some(root))) ++ Seq(Some(pts)))
      val jsonState = zkClient.getData.forPath(pathToData)
      val state = Json.parse(jsonState)
      val offset = (state \ "offset").as[Long]
      val partition = (state \ "partition").as[Long]
      (partition.toInt, offset)
    }).toMap
  }

  def getKafkaState(topic: String): TopicInfo = {
   utils.KafkaApi.getKafkaState(topic)
  }
  def getKafkaState(topics: List[String]): List[TopicInfo] = {
    utils.KafkaApi.getKafkaState(topics)
  }

  def getTopologyDeltas(topoRoot: String, topic: String): Tuple2[Long, List[Delta]] = {
    val stormState = ZkKafka.getSpoutState(topoRoot, topic)

    val zkState = ZkKafka.getKafkaState(topic).partitions

    var total = 0L;
    val deltas = zkState.map({ partitionInfo =>
      val partition = partitionInfo.id
      val koffset = partitionInfo.offset
      stormState.get(partition) map { soffset =>
        val amount = koffset - soffset
        total = amount + total
        Delta(partition = partition, amount = Some(amount), current = koffset, storm = Some(soffset))
      } getOrElse(
        Delta(partition = partition, amount = None, current = koffset, storm = None)
        )
    }).toList.sortBy(_.partition)

    (total, deltas)
  }

  /**
   * get all topics's info from zookeeper
   * @return
   */
  def listTopics : List[TopicInfo] = {
    val iter = zkClient.getChildren.forPath("/brokers/topics").iterator()
    var topicList = List.empty[String]
    while(iter.hasNext)
      topicList = iter.next() :: topicList
    val currentTime: Long = System.currentTimeMillis()
    //get offsets for each topic
    val topicInfos = getKafkaState(topicList) // store topic states


    //now topics has got all information of current topics. Begin to config if a topic is active
    var topicStates = List.empty[TopicInfo] //用于保存经过此次处理后得到的topic的信息。用于返回给view
    topicInfos foreach { topicInfo =>
      var topicState = topicInfo
      _topics foreach { cachedTopics =>
        val cachedTopic = cachedTopics.getOrElse(topicInfo.topicName, topicInfo)
        if(cachedTopic.total != topicInfo.total){
          val interval: Int = ((topicInfo.timestamp - cachedTopic.timestamp) / 1000).toInt
          var partitionStates = List.empty[PartitionInfo] //计算现在的partition每个的offset增长了多少
//          topicInfo.partitions.zipWithIndex.foreach{
          //            case (currentPartState, index) => {
          //              partitionStates = PartitionInfo(currentPartState.partition, currentPartState.offset,
          //              currentPartState.leader, currentPartState.isr,
          //                Increment(interval,
          //                  (currentPartState.offset - cachedTopic.partitions(index).offset).toInt
          //                )
          //              ) :: partitionStates
          //            }
          //          }
          val cachedPartitionInfos = cachedTopic.partitions.map{p => (p.id, p)}.toMap
          topicInfo.partitions.foreach{partition =>
            val id = partition.id
            val cachedPartition = cachedPartitionInfos.getOrElse(id, partition) //if this partition didn't exists, return its current state
            partitionStates = PartitionInfo(topicInfo.topicName, id, partition.offset
            ,partition.leader, partition.reps, partition.isr, Increment(interval, (partition.offset - cachedPartition.offset).toInt)) :: partitionStates
          }
          partitionStates = partitionStates.sortBy(_.id)
          topicState = TopicInfo(currentTime, topicInfo.topicName, partitionStates
            , topicInfo.total, Increment(interval, topicInfo.total - cachedTopic.total), true)
        }
      }
      topicStates = topicState :: topicStates
    }
    var cachedTopicInfo = Map.empty[String, TopicInfo]
    topicStates.foreach { topicInfo =>
      cachedTopicInfo += (topicInfo.topicName -> topicInfo)
    }
    _topics = Some(cachedTopicInfo)
    topicStates
  }
}