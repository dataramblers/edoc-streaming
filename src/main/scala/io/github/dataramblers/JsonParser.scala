package io.github.dataramblers

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import org.json4s._

sealed abstract class Person {
  val family: Option[String]
  val given: Option[String]
}

case class Author(family: Option[String],
                  given: Option[String]) extends Person
case class Editor(family: Option[String],
                         given: Option[String]) extends Person

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
                score: Option[Double],
                results: Option[Integer])

object JsonParser {

  def toEdoc(jsonString: String): Edoc = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    parse(jsonString).extract[Edoc]
  }

}
