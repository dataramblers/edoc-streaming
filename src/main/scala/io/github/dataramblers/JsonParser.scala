package io.github.dataramblers

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import org.json4s._

case class Person(family: String,
                  given: String)

case class Edoc(creators: Option[List[Person]],
                editors: Option[List[Person]],
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
