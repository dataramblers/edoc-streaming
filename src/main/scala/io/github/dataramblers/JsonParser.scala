package io.github.dataramblers

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import org.json4s._


case class Name(given: Option[String], family: Option[String])

sealed abstract class Person {
  val name: Name
}

case class Author(name: Name) extends Person
case class Editor(name: Name) extends Person

case class Edoc(creators: Option[List[Author]],
                editors: Option[List[Editor]],
                title: Option[String],
                isbn: Option[String],
                isbn_e: Option[String],
                issn: Option[String],
                issn_e: Option[String],
                doi: Option[String],
                pmid: Option[String],
                eprintid: Integer,
                date: Option[Int],
                score: Option[Double],
                results: Option[Integer]) extends Throwable



object JsonParser {

  def toEdoc(jsonString: String): Edoc = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    parse(jsonString).extract[Edoc]
  }

}
