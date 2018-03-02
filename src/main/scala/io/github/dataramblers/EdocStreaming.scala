package io.github.dataramblers

import java.io.{BufferedInputStream, FileInputStream}
import java.util.zip.GZIPInputStream

import com.sksamuel.elastic4s.Indexable
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.scala.Logging

import scala.io.Source
import scala.util.{Failure, Success, Try}


object EdocStreaming extends Logging {

  def jsonRef(field_name: String, field: Option[AnyRef]): String = {
    field match {
      case Some(x) => "\"" + field_name + "\": \"" + x.toString + "\", "
      case None => ""
    }
  }

  def jsonRefInt(field_name: String, field: Option[AnyRef]): String = {
    field match {
      case Some(x) => "\"" + field_name + "\": " + x.toString + ", "
      case None => ""
    }
  }

  def jsonVal(field_name: String, field: Option[AnyVal]): String = {
    field match {
      case Some(x) => "\"" + field_name + "\": " + x.toString + ", "
      case None => ""
    }
  }

  def name(field: Option[String]): String = field match {
    case Some(x) => x;
    case None => ""
  }

  def jsonPerson(p: Person): String = "{\"name\": {\"family\": \"" + name(p.name.family) + "\", \"given\": \"" + name(p.name.given) + "\" }}"

  def jsonList(field_name: String, field: Option[List[Person]]): String = {
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

  implicit object Edoc extends Indexable[Edoc] {
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

    val config = ConfigFactory.load()

    logger.info("Loading and parsing edoc file")
    val edocRecords: Array[String] = Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(config.getString("sources.edoc-path"))))).mkString.split("\n")
    val asJson = edocRecords.map(x => JsonParser.toEdoc(x))

    for {
      boostAndFuzzinessValues <- BoostFuzzinessIterator.initialize(config).zipWithIndex
      edoc <- asJson
    } perform(edoc, boostAndFuzzinessValues._1, (config.getInt("output.index-offset-counter") + boostAndFuzzinessValues._2).toString)

    logger.info("FINISHED COMPARISON")

    def perform(edoc: Edoc, boostAndFuzzinessValues: (String, Double, String, Double, Double, Double, Double), esIndexCounter: String): Unit = {
      logger.info(s"Starting indexing with the following values: title fuzziness: ${boostAndFuzzinessValues._1}; " +
        s"title boost: ${boostAndFuzzinessValues._2}; person fuzziness: ${boostAndFuzzinessValues._3}; " +
        s"person boost: ${boostAndFuzzinessValues._4}; isbn boost: ${boostAndFuzzinessValues._5}; " +
        s"issn boost: ${boostAndFuzzinessValues._6}; date boost: ${boostAndFuzzinessValues._7}")
      logger.info(s"Indexing into ${config.getString("output.index") + esIndexCounter} / ${config.getString("output.type")}")
      val execute = Try(ESLookup.lookup(edoc, config.getString("sources.crossref-index"), config.getString("sources.crossref-type"), boostAndFuzzinessValues))
      execute match {
        case Success(v) =>
          ESLookup.client.execute {
            indexInto(config.getString("output.index") + esIndexCounter / config.getString("output.type")).id(v.get.eprintid).doc(v.get)
          }
          logger.debug("[SUCCESS] " + v.get.eprintid)
        case Failure(e) => logger.error("[FAILED] ${e.getMessage}")
      }
    }

  }
}
