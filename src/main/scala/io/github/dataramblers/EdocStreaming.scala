package io.github.dataramblers

import java.io.{BufferedInputStream, FileInputStream}
import java.util.zip.GZIPInputStream

import com.sksamuel.elastic4s.Indexable
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.scala.Logging
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.{Formats, NoTypeHints}

import scala.io.Source


object EdocStreaming extends Logging {

  private def jsonRef(field_name: String, field: Option[AnyRef]) = {
    field match {
      case Some(x) => "\"" + field_name + "\": \"" + x.toString + "\", "
      case None => ""
    }
  }

  private def jsonRefInt(field_name: String, field: Option[AnyRef]) = {
    field match {
      case Some(x) => "\"" + field_name + "\": " + x.toString + ", "
      case None => ""
    }
  }

  private def jsonVal(field_name: String, field: Option[AnyVal]) = {
    field match {
      case Some(x) => "\"" + field_name + "\": " + x.toString + ", "
      case None => ""
    }
  }

  private def jsonList(field_name: String, field: Option[List[Person]]) = {
    field match {
      case Some(x) =>
        var result = "\"" + field_name + "\": ["
        for (p <- x.tail)
          result += jsonPerson(p) + ","
        result += jsonPerson(x.head)
        result = result + "]"
        result
      case None => ""
    }
  }

  private def jsonPerson(p: Person) = "{\"name\": {\"family\": \"" + name(p.name.family) + "\", \"given\": \"" + name(p.name.given) + "\" }}"

  private def name(field: Option[String]) = field match {
    case Some(x) => x;
    case None => ""
  }

  implicit private object Edoc extends Indexable[Edoc] {
    override def json(t: Edoc): String = {
      var result: String = "{"
      result += jsonRef("title", t.title)
      result += jsonRef("isbn", t.isbn)
      result += jsonRef("isbn_e", t.isbn_e)
      result += jsonRef("issn", t.issn)
      result += jsonRef("issn_e", t.issn_e)
      result += jsonRef("doi", t.doi)
      result += jsonRefInt("hits", t.results)
      result += jsonVal("date", t.date)
      result += jsonVal("score", t.score)
      result += "\"eprintid\": " + t.eprintid.toString + ", "
      result += jsonList("creators", t.creators)
      result += jsonList("editors", t.editors)
      result += "}"
      result
    }
  }

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.parseString((
      if (args.length > 0) Source.fromFile(args(0))
      else Source.fromInputStream(getClass.getResourceAsStream("/application.json"))).mkString
    )

    logger.info("Loading and parsing edoc file")
    val edocRecords: Array[String] = Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(config.getString("sources.edoc-path"))))).mkString.split("\n")
    val asJson = edocRecords.map(x => JsonParser.toEdoc(x))

    ElasticsearchClient.setup(config.getString("elasticsearch.host"), config.getInt("elasticsearch.port"))
    Utilities.createEsIndex(config.getString(
      "output.index-prefix") + config.getString("output.index-counter-offset"),
      config.getString("output.type")
    )

    for {
      boostAndFuzzinessValues <- BoostFuzzinessIterator.initialize(config).zipWithIndex
      edoc <- asJson
    } perform(edoc, boostAndFuzzinessValues._1, (config.getInt("output.index-counter-offset") + boostAndFuzzinessValues._2).toString)

    logger.info("FINISHED COMPARISON")

    def perform(edoc: Edoc, boostAndFuzzinessValues: (String, Double, String, Double, Double, Double, Double), esIndexCounter: String): Unit = {
      logger.info(s"Starting indexing with the following values: title fuzziness: ${boostAndFuzzinessValues._1}; " +
        s"title boost: ${boostAndFuzzinessValues._2}; person fuzziness: ${boostAndFuzzinessValues._3}; " +
        s"person boost: ${boostAndFuzzinessValues._4}; isbn boost: ${boostAndFuzzinessValues._5}; " +
        s"issn boost: ${boostAndFuzzinessValues._6}; date boost: ${boostAndFuzzinessValues._7}")
      logger.info(s"Indexing into ${config.getString("output.index-prefix") + esIndexCounter} / ${config.getString("output.type")}")
      val edoc_updated = ESLookup.lookup(edoc, config, boostAndFuzzinessValues)
      implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
      ElasticsearchClient.getClient.execute {
        indexInto((config.getString("output.index-prefix") + esIndexCounter) / config.getString("output.type")).id(edoc_updated.eprintid).doc(write(edoc_updated))
      }
    }

  }
}
