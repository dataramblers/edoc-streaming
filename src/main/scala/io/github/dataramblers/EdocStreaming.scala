package io.github.dataramblers

import java.io.{BufferedInputStream, FileInputStream}
import java.util.zip.GZIPInputStream

import com.sksamuel.elastic4s.Indexable
import com.sksamuel.elastic4s.http.ElasticDsl._

import scala.io.Source
import scala.util.{Failure, Success, Try}


object EdocStreaming {

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
    val path = "/home/swissbib/PycharmProjects/uni_basel/crossrefproject/edoc-data.json.gz"
    val edocRecords: Array[String] = Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(path)))).mkString.split("\n")

    val asJson = edocRecords.map(x => JsonParser.toEdoc(x))

    def perform(edoc: Edoc): Unit = {
      val execute = Try(ESLookup.lookup(edoc, "crossref_v2", "crossref"))
      execute match {
        case Success(v) =>
          ESLookup.client.execute {
            indexInto("compare_v4" / "result").id(v.get.eprintid).doc(v.get)
          }
          println("[SUCCESS] " + v.get.eprintid)
        case Failure(e) => println("[FAILED] " + e.getMessage)
      }
    }

    for (edoc <- asJson) perform(edoc)

    println("FINISHED COMPARISON")
    System.exit(0)
  }
}
