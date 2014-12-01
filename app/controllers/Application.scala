package controllers

import akka.dispatch.sysmsg.Failed
import com.codahale.metrics.json.MetricsModule
import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.fasterxml.jackson.databind.{ObjectWriter, ObjectMapper}
import com.yammer.metrics.reporting.DatadogReporter
import java.io.StringWriter
import java.util.concurrent.TimeUnit
import models.Metrics
import models.ZkKafka
import play.api.Play.current
import play.api._
import play.api.libs.json.{JsObject, Json, JsValue}
import play.api.mvc._
import utils.KafkaApi
import utils.StormApi
import scala.language.implicitConversions
import scala.util.{Try, Success}

object Application extends Controller {

  val validUnits = Some(Set("NANOSECONDS", "MICROSECONDS", "MILLISECONDS", "SECONDS", "MINUTES", "HOURS", "DAYS"))
  val mapper = new ObjectMapper()

  def registryName = Play.configuration.getString("capillary.metrics.name").getOrElse("default")
  def rateUnit     = Play.configuration.getString("capillary.metrics.rateUnit", validUnits).getOrElse("SECONDS")
  def durationUnit = Play.configuration.getString("capillary.metrics.durationUnit", validUnits).getOrElse("SECONDS")
  def showSamples  = Play.configuration.getBoolean("capillary.metrics.showSamples").getOrElse(false)
  def ddAPIKey     = Play.configuration.getString("capillary.metrics.datadog.apiKey")

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

  implicit def stringToTimeUnit(s: String) : TimeUnit = TimeUnit.valueOf(s)

  def index = Action { implicit request =>

    val topos = ZkKafka.getTopologies

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

    Ok(views.html.index(topos, topics))
  }

  def brokers = Action { implicit requst =>
    val topics = ZkKafka.listTopics
    val brokers = KafkaApi.getPartitionDistribution(topics)
    Ok(views.html.brokers(brokers))
  }

  def topo(name: String, topoRoot: String, topic: String) = Action { implicit request =>

    val totalAndDeltas = ZkKafka.getTopologyDeltas(topoRoot, topic)

    Ok(views.html.topology(name, topic, totalAndDeltas._1, totalAndDeltas._2.toSeq))
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
    val cluster = StormApi.getClusterSummary()
    val supervisor = StormApi.getSupervisorSummary()
    val topologies = StormApi.getTopologySummary()
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