package io.github.dataramblers

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.searches.queries.BoolQueryDefinition
import com.sksamuel.elastic4s.searches.queries.matches.MatchQueryDefinition
import com.sksamuel.elastic4s.{Hit, HitReader}
import com.typesafe.config.Config
import org.apache.logging.log4j.scala.Logging
import org.elasticsearch.index.query.Operator

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Success, Try}

object ESLookup extends Logging {

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

  def lookup(edoc: Edoc, config: Config, boostAndFuzziness: (String, Double, String, Double, Double, Double, Double)): Try[Edoc] = {
    //val query = search(index / doctype) query buildQuery(edoc)
    //println(client.show(search(index / doctype) query buildQuery(edoc)))
    val result = ElasticsearchClient.getClient.execute {
      search(config.getString("sources.crossref-index") / config.getString("sources.crossref-type")) query buildQuery(edoc, boostAndFuzziness)
    }
    val sResponse: SearchResponse = Await.result(result, 20.seconds)
    if (sResponse.maxScore > 0) {
      logger.debug(search(config.getString("sources.crossref-index") / config.getString("sources.crossref-type")) query buildQuery(edoc, boostAndFuzziness))
      logger.debug(s"${sResponse.maxScore}")
      logger.debug(s"${sResponse.hits.total}")
    }
    val serialisedResult = sResponse.to[Crossref]
    // TODO: Redefine threshold
    if (sResponse.totalHits > 0 && sResponse.maxScore >= 1.1) {
      sResponse.hits.hits(0)
      Success[Edoc](edoc.copy(doi = Some(serialisedResult(0).DOI), score = Some(sResponse.maxScore), results = Some(sResponse.totalHits)))
    }
    else
      throw new NoMatchFound("Could not match edoc record " + edoc.eprintid.toString, edoc)
  }

  private def buildQuery(edoc: Edoc, boostAndFuzziness: (String, Double, String, Double, Double, Double, Double)): BoolQueryDefinition = {
    BoolQueryDefinition(
      must = Seq(
        titleMatch(edoc.title, boostAndFuzziness._2, boostAndFuzziness._1)).flatten ++
        personNameMatch(edoc.editors, "editor", boostAndFuzziness._4, boostAndFuzziness._3).getOrElse(Seq()) ++
        personNameMatch(edoc.creators, "author", boostAndFuzziness._4, boostAndFuzziness._3).getOrElse(Seq()),
      should = Seq(
        issnMatch(edoc.issn, boostAndFuzziness._6),
        issnMatch(edoc.issn_e, boostAndFuzziness._6),
        isbnMatch(edoc.isbn, boostAndFuzziness._5),
        isbnMatch(edoc.isbn_e, boostAndFuzziness._5),
        dateMatch(edoc.date, "published-online", boostAndFuzziness._7),
        dateMatch(edoc.date, "published-print", boostAndFuzziness._7)
      ).flatten
    )
  }

  private def titleMatch(title: Option[String], boost: Double, fuzziness: String): Option[MatchQueryDefinition] = {
    title match {
      case Some(t) =>
        Some(MatchQueryDefinition(field = "title", value = t, boost = Some(boost), fuzziness = Some(fuzziness), operator = Some(Operator.AND)))
      case None =>
        None
    }
  }

  private def personNameMatch(person: Option[List[Person]], parentField: String, boost: Double, fuzziness: String): Option[Seq[MatchQueryDefinition]] = person match {
    case Some(pers) => Some(pers flatMap (x => {
      (if (x.name.given.isDefined) Seq(MatchQueryDefinition(field = parentField + ".given", value = x.name.given.get, boost = Some(boost), fuzziness = Some(fuzziness), operator = Some(Operator.AND))) else Seq()) ++
        (if (x.name.family.isDefined) Seq(MatchQueryDefinition(field = parentField + ".family", value = x.name.family.get, boost = Some(boost), fuzziness = Some(fuzziness), operator = Some(Operator.AND))) else Seq())
    }))
    case None =>
      None
  }

  private def isbnMatch(isbn: Option[String], boost: Double): Option[MatchQueryDefinition] = {
    isbn match {
      case Some(i) =>
        // Ensure that only valid isbn's come as input.
        Some(MatchQueryDefinition(field = "isbn", value = Utilities.isbn10ToIsbn13(i), boost = Some(boost), fuzziness = None, operator = Some(Operator.AND)))
      case None =>
        None
    }
  }

  private def issnMatch(issn: Option[String], boost: Double): Option[MatchQueryDefinition] = {
    issn match {
      case Some(t) if Utilities.isValidISSN(t) =>
        Some(MatchQueryDefinition(field = "issn", value = t, boost = Some(boost), fuzziness = None, operator = Some(Operator.AND)))
      case _ =>
        None
    }
  }

  private def dateMatch(date: Option[Int], fieldName: String, boost: Double): Option[MatchQueryDefinition] = {
    date match {
      case Some(d) => Some(MatchQueryDefinition(field = fieldName, value = d, boost = Some(boost)))
      case None => None
    }
  }

  private def optionInt(field: Option[AnyRef]): Option[Int] = {
    field match {
      case Some(x) => Some(x.asInstanceOf[Int])
      case None => None
    }
  }

  private def defaultEmptyVal(field: Option[AnyRef]): String = {
    field match {
      case Some(x) => x.toString
      case None => ""
    }
  }

  private def defaultEmptyBoolean(field: Option[AnyRef]): Boolean = {
    field match {
      case Some(x) => x.toString.toBoolean
      case None => false
    }
  }

  class NoMatchFound(message: String, cause: Throwable) extends Exception(message, cause)

}