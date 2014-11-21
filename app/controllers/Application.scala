package controllers

import com.codahale.metrics.json.MetricsModule
import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.fasterxml.jackson.databind.{ObjectWriter, ObjectMapper}
import com.yammer.metrics.reporting.DatadogReporter
import java.io.StringWriter
import java.util.concurrent.TimeUnit
import models.Metrics
import models.ZkKafka
import models.ZkKafka._
import play.api.Play.current
import play.api._
import play.api.mvc._
import scala.language.implicitConversions

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
    topics = topics.sortBy(_.topic)

    Ok(views.html.index(topos, topics))
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
}