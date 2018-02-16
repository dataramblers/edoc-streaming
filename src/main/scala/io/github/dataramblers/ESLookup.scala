package io.github.dataramblers

import com.sksamuel.elastic4s.{ElasticsearchClientUri, Hit, HitReader}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.searches.queries.BoolQueryDefinition
import com.sksamuel.elastic4s.searches.queries.matches.MatchQueryDefinition
import org.elasticsearch.index.query.Operator
import scala.concurrent.duration._

import scala.concurrent.Await
import scala.util.{Failure, Success, Try}

object ESLookup {

  val client = HttpClient(ElasticsearchClientUri("localhost", 8080))

  def optionInt(field: Option[AnyRef]): Option[Int] = {
    field match {
      case Some(x) => Some(x.asInstanceOf[Int])
      case None => None
    }
  }

  def defaultEmptyVal(field: Option[AnyRef]): String = {
    field match {
      case Some(x) => x.toString
      case None => ""
    }
  }

  def defaultEmptyBoolean(field: Option[AnyRef]): Boolean = {
    field match {
      case Some(x) => x.toString.toBoolean
      case None => false
    }
  }

  implicit object CrossrefHitReader extends HitReader[Crossref] {
    override def read(hit: Hit): Either[Throwable, Crossref] = {
      Right(Crossref(defaultEmptyVal(hit.sourceFieldOpt("DOI")),
        defaultEmptyVal(hit.sourceFieldOpt("ISBN")),
        defaultEmptyVal(hit.sourceFieldOpt("ISSN")),
        CrossrefPerson(
          defaultEmptyVal(hit.sourceFieldOpt("author.ORCID")),
          defaultEmptyBoolean(hit.sourceFieldOpt("author.`authenticated-orcid`")),
          defaultEmptyVal(hit.sourceFieldOpt("author.family")),
          defaultEmptyVal(hit.sourceFieldOpt("author.given")),
          defaultEmptyVal(hit.sourceFieldOpt("author.name")),
          defaultEmptyVal(hit.sourceFieldOpt("author.suffix"))
        ),
        CrossrefPerson(
          defaultEmptyVal(hit.sourceFieldOpt("editor.ORCID")),
          defaultEmptyBoolean(hit.sourceFieldOpt("editor.`authenticated-orcid`")),
          defaultEmptyVal(hit.sourceFieldOpt("editor.family")),
          defaultEmptyVal(hit.sourceFieldOpt("editor.given")),
          defaultEmptyVal(hit.sourceFieldOpt("editor.name")),
          defaultEmptyVal(hit.sourceFieldOpt("editor.suffix"))
        ),
        defaultEmptyVal(hit.sourceFieldOpt("title")),
        optionInt(hit.sourceFieldOpt("published-online")),
        optionInt(hit.sourceFieldOpt("published-print"))
      ))
    }
  }

  def lookup(edoc: Edoc, index: String, doctype: String): Try[Edoc] = {
    //val query = search(index / doctype) query buildQuery(edoc)
    //println(client.show(search(index / doctype) query buildQuery(edoc)))
    val result = client.execute {
      search(index / doctype) query buildQuery(edoc)
    }
    val sResponse: SearchResponse =  Await.result(result, 20.seconds)
    /*
    if (sResponse.maxScore > 0) {
      println(client.show(search(index / doctype) query buildQuery(edoc)))
      println(sResponse.maxScore)
      println(sResponse.hits.total)
    }
    */
    val serialisedResult = sResponse.to[Crossref]
    // TODO: Redefine threshold
    if (sResponse.totalHits > 0 && sResponse.maxScore >= 1.1) {
      sResponse.hits.hits(0)
      Success[Edoc](edoc.copy(doi = Some(serialisedResult(0).DOI), score = Some(sResponse.maxScore), results = Some(sResponse.totalHits)))
    }
    else
      throw new NoMatchFound("Could not match edoc record " + edoc.eprintid.toString, edoc)
  }

  private def buildQuery(edoc: Edoc): BoolQueryDefinition = {
    BoolQueryDefinition(
      must = Seq(
        titleMatch(edoc.title)).flatten ++
        personNameMatch(edoc.editors, "editor").getOrElse(Seq()) ++
        personNameMatch(edoc.creators, "author").getOrElse(Seq()),
      should = Seq(
        issnMatch(edoc.issn),
        issnMatch(edoc.issn_e),
        isbnMatch(edoc.isbn),
        isbnMatch(edoc.isbn_e),
        dateMatch(edoc.date, "published-online"),
        dateMatch(edoc.date, "published-print")
      ).flatten
    )
  }

  private def titleMatch(title: Option[String]): Option[MatchQueryDefinition] = {
    title match {
      case Some(t) =>
        Some(MatchQueryDefinition(field = "title", value = t, boost = Some(1), fuzziness = Some("5"), operator = Some(Operator.AND)))
      case None =>
        None
    }
  }

  private def personNameMatch(person: Option[List[Person]], parentField: String): Option[Seq[MatchQueryDefinition]] = person match {
    case Some(pers) => Some(pers flatMap (x => {
      // TODO: Find reasonable values for boost and fuzziness
      (if (x.name.given.isDefined) Seq(MatchQueryDefinition(field = parentField + ".given", value = x.name.given.get, boost = Some(1), fuzziness = None, operator = Some(Operator.AND))) else Seq()) ++
        (if (x.name.family.isDefined) Seq(MatchQueryDefinition(field = parentField + ".family", value = x.name.family.get, boost = Some(1), fuzziness = None, operator = Some(Operator.AND))) else Seq())
    }))
    case None =>
      None
  }



  private def isbnMatch(isbn: Option[String]): Option[MatchQueryDefinition] = {
    isbn match {
      case Some(i) =>
        // TODO: Find reasonable values for boost and fuzziness
        // Ensure that only valid isbn's come as input.
        Some(MatchQueryDefinition(field = "isbn", value = Utilities.isbn10ToIsbn13(i), boost = Some(10), fuzziness = None, operator = Some(Operator.AND)))
      case None =>
        None
    }
  }

  // Small calculation whether a ISSN has a valid format.
  private def isValidISSN(issn: String): Boolean = {
    val regex = "^\\d{4}-\\d{3}[\\dxX]$".r
    regex.findFirstIn(issn) match {
      case Some(t) =>
        val asInt = Try(t.charAt(9).toInt)
        def check(result: Try[Int]): Int = {
          asInt match {
            case Success(x) => x
            case Failure(_) => 10
          }
        }
        val total = t(0).toInt * 8 + t(1).toInt * 7 + t(2).toInt * 6 + t(3).toInt * 5 + t(5) * 4 + t(6) * 3 + t(7) * 2
        val checkDigit = 11 - (total % 11)
        if (checkDigit == check(asInt)) true
        else false
      case None =>
        false
    }
  }

  private def issnMatch(issn: Option[String]): Option[MatchQueryDefinition] = {
    issn match {
      case Some(t) =>
        // TODO: Find reasonable values for boost and fuzziness
        if (isValidISSN(t))
          Some(MatchQueryDefinition(field = "issn", value = t, boost = Some(10), fuzziness = None, operator = Some(Operator.AND)))
        else
          None
      case None =>
        None
    }
  }

  private def dateMatch(date: Option[Int], fieldName: String): Option[MatchQueryDefinition] = {
    date match {
      case Some(d) => Some(MatchQueryDefinition(field = fieldName, value = d, boost = Some(10)))
      case None => None
    }
  }


  class NoMatchFound(message: String, cause: Throwable) extends  Exception(message, cause)

}