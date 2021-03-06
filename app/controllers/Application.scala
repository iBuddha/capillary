package controllers

import akka.actor.Props
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.{ObjectWriter, ObjectMapper}
import com.ning.http.client.AsyncHttpClient
import com.yammer.metrics.reporting.DatadogReporter
import java.io.StringWriter
import java.util.concurrent.TimeUnit
import models.Metrics
import models.ZkKafka
import models.actors.{TopoQueue, CurrentTopologiesReader, StormClusterStatesMonitor}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import play.api.Play.current
import play.api._
import play.api.libs.json.{JsObject, Json, JsValue}
import play.api.mvc._
import utils.KafkaApi
import utils.StormApi
import scala.concurrent.{Future, Await}
import scala.language.implicitConversions
import scala.util.{Try, Success, Failure}
import play.api.libs.concurrent.Akka
import play.api.libs.concurrent.Execution.Implicits._
import scala.concurrent.duration._
import akka.pattern.Patterns.ask
import scala.language.postfixOps

object Application extends Controller {

  val validUnits = Some(Set("NANOSECONDS", "MICROSECONDS", "MILLISECONDS", "SECONDS", "MINUTES", "HOURS", "DAYS"))
  val mapper = new ObjectMapper()

  def registryName = Play.configuration.getString("capillary.metrics.name").getOrElse("default")
  def rateUnit     = Play.configuration.getString("capillary.metrics.rateUnit", validUnits).getOrElse("SECONDS")
  def durationUnit = Play.configuration.getString("capillary.metrics.durationUnit", validUnits).getOrElse("SECONDS")
  def showSamples  = Play.configuration.getBoolean("capillary.metrics.showSamples").getOrElse(false)
  def ddAPIKey     = Play.configuration.getString("capillary.metrics.datadog.apiKey")

  //CuratorFramework instance is fully thread safe, so initialize it here once, and share it whenever needed
  val zookeepers = Play.configuration.getString("capillary.zookeepers").getOrElse("localhost:2181")
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  implicit val zkClient = CuratorFrameworkFactory.newClient(zookeepers, retryPolicy)
  zkClient.start()

  val module = new MetricsModule(rateUnit, durationUnit, showSamples)
  mapper.registerModule(module)

  ddAPIKey.map({ apiKey =>
    Logger.info("Starting Datadog Reporter")
    val reporter = new DatadogReporter.Builder()
      .withApiKey(apiKey)
      // .withMetricNameFormatter(ShortenedNameFormatter)
      .build()
    reporter.start(20, TimeUnit.SECONDS)
  })

  val stormClusterMonitor = Akka.system.actorOf(Props[StormClusterStatesMonitor], name = "storm-cluster-monitor")
  val topoReader = Akka.system.actorOf(Props[CurrentTopologiesReader], "topology-reader")
  val topologyCheckInterval = Play.configuration.getInt("capillary.monitors.topology.checkInterval.seconds").getOrElse(30)
  val clusterCheckInterval = Play.configuration.getInt("capillary.monitors.cluster.checkInterval.seconds").getOrElse(10)
  Akka.system.scheduler.schedule(0.microsecond, clusterCheckInterval.seconds, stormClusterMonitor, StormClusterStatesMonitor.Tik)
  Akka.system.scheduler.schedule(0.microsecond, topologyCheckInterval.seconds, topoReader, CurrentTopologiesReader.Tik)

  implicit def stringToTimeUnit(s: String) : TimeUnit = TimeUnit.valueOf(s)

  def index = Action { implicit request =>

    val allAliveTopos = StormApi.getTopologySummary().get //get all topologies, whatever using Kafka Spout
    val kafkaTopos = ZkKafka.getTopologies
    val topoStates = allAliveTopos.map(t => (t.name, t.status)).toMap
    val topoAndStates = kafkaTopos.filter(t => topoStates.contains(t.name)).map(t => (t, topoStates.get(t.name).get)).toMap

    var topics = ZkKafka.listTopics
    topics = topics.sortWith{
      case (topicA, topicB) => {
      var result = false
        if(topicA.isActive != topicB.isActive)
          result = topicA.isActive > topicB.isActive
        else if(topicA.topicName != topicB.topicName)
          result = topicA.topicName < topicB.topicName
      result
      }
    }

    Ok(views.html.index(topoAndStates, kafkaTopos, topics))
  }

  def d3 = Action{implicit  request =>
    Ok(views.html.d3(
      """
        [
        {"date" : "12-11:10", "close":98200001},
        {"date" : "12-12:11", "close":98200002},
        {"date" : "12-13:12", "close":98200005},
        {"date" : "12-14:13", "close":98200006},
        {"date" : "12-15:15", "close":98200011},
        {"date" : "12-16:16", "close":98200022},
        {"date" : "12-17:17", "close":98200035},
       {"date" : "12-18:18", "close":98200046}
        ]
      """))
  }

  def brokers = Action { implicit requst =>
    val topics = ZkKafka.listTopics
    val brokers = KafkaApi.getPartitionDistribution(topics)
    Ok(views.html.brokers(brokers))
  }

  def topo(name: String, topoRoot: String, topic: String) = Action.async { implicit request =>

    val totalAndDeltas = ZkKafka.getTopologyDeltas(topoRoot, topic)
    val incrFuture = ask(topoReader, CurrentTopologiesReader.OffsetIncr(name), 20 seconds)
    val incr: Future[Option[TopoQueue.Increment]]= incrFuture.map{
      case r: TopoQueue.Increment => Some(r)
      case _ => None
    }
    val timeoutFuture = play.api.libs.concurrent.Promise.timeout("Oops", 20.second)
    Future.firstCompletedOf(Seq(incr, timeoutFuture)).map {
      case i: Option[TopoQueue.Increment]=> Ok(views.html.topology(name, topic, totalAndDeltas._1, totalAndDeltas._2.toSeq, i))
//      case t: String => InternalServerError(t)
    }
//    Ok(views.html.topology(name, topic, totalAndDeltas._1, totalAndDeltas._2.toSeq, incr))
  }

  def metrics = Action {
    val writer: ObjectWriter = mapper.writerWithDefaultPrettyPrinter()
    val stringWriter = new StringWriter()
    writer.writeValue(stringWriter, Metrics.metricRegistry)
    Ok(stringWriter.toString).as("application/json").withHeaders("Cache-Control" -> "must-revalidate,no-cache,no-store")
  }

  def stormConfig = Action { implicit  request =>
    val config: Try[JsValue] = StormApi.getConfiguration()
//    val fieldSet: Set[(String, JsValue)] = config.asInstanceOf[JsObject].fieldSet.toSet
//    Ok(views.html.stormConfig(fieldSet))
    config.map{ conf =>
      val fieldSet: Set[(String, JsValue)] = conf.asInstanceOf[JsObject].fieldSet.toSet
          Ok(views.html.stormConfig(fieldSet))
    }.get
  }
  def stormSummary = Action { implicit request =>
    val httpClient = new AsyncHttpClient()
    val cluster = StormApi.getClusterSummary(httpClient)
    val supervisor = StormApi.getSupervisorSummary(httpClient)
    val topologies = StormApi.getTopologySummary(httpClient)
    httpClient.close()
//    (for {
//        c <- cluster
//        s <- supervisor
//        t <- topologies
//      } yield Ok(views.html.storm(c, s, t))).get
    val c = cluster.getOrElse(StormApi.failedClusterSummary("error when retrive cluster summay"))
    val s = supervisor.getOrElse(List(StormApi.failedSupervisorSummary("error when retrive supervisor summary")))
    val t = topologies.getOrElse(List(StormApi.failedTopologySummary("error when retrive topology summary")))
    Ok(views.html.storm(c, s ,t))
  }
}