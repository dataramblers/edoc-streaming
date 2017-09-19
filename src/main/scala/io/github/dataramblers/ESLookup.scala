package io.github.dataramblers

import com.sksamuel.elastic4s.{ElasticsearchClientUri, Hit, HitReader}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.searches.queries.BoolQueryDefinition
import com.sksamuel.elastic4s.searches.queries.matches.MatchQueryDefinition
import org.elasticsearch.index.query.Operator

object ESLookup {

  val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

  implicit object CrossrefHitReader extends HitReader[Crossref] {
    override def read(hit: Hit): Either[Throwable, Crossref] = {
      Right(Crossref(hit.sourceAsMap("DOI").toString,
        hit.sourceAsMap("ISBN").toString,
        hit.sourceAsMap("ISSN").toString,
        CrossrefPerson(
          hit.sourceAsMap("author.ORCID").toString,
          hit.sourceAsMap("author.`authenticated-orcid`").toString.toBoolean,
          hit.sourceAsMap("author.family").toString,
          hit.sourceAsMap("author.given").toString,
          hit.sourceAsMap("author.name").toString,
          hit.sourceAsMap("author.suffix").toString
        ),
        CrossrefPerson(
          hit.sourceAsMap("editor.ORCID").toString,
          hit.sourceAsMap("editor.`authenticated-orcid`").toString.toBoolean,
          hit.sourceAsMap("editor.family").toString,
          hit.sourceAsMap("editor.given").toString,
          hit.sourceAsMap("editor.name").toString,
          hit.sourceAsMap("editor.suffix").toString
        ),
        hit.sourceAsMap("title").toString))
    }
  }

  def lookup(edoc: Edoc, index: String, doctype: String): Edoc = {
    println((search(index / doctype) query buildQuery(edoc, index, doctype)).toString)
    val sResponse: SearchResponse = client.execute {
      search(index / doctype) query buildQuery(edoc, index, doctype)
    }.await
    println(sResponse.totalHits)
    val serialisedResult = sResponse.to[Crossref]
    // TODO: Is it required to wrap Crossref case class in Option in order to prevent exceptions if respective field is missing?
    val doi = if (sResponse.totalHits > 0) serialisedResult(0).DOI else ""
    if (sResponse.maxScore > 1.1 && doi != "")
      edoc.copy(doi = Some(doi), score = Some(sResponse.maxScore), results = Some(sResponse.totalHits))
    else
      edoc
  }

  private def buildQuery(edoc: Edoc, index: String, doctype: String): BoolQueryDefinition = {
    BoolQueryDefinition(must = Seq(
      titleMatch(edoc.title),
      issnMatch(edoc.issn),
      issnMatch(edoc.issn_e),
      isbnMatch(edoc.isbn),
      isbnMatch(edoc.isbn_e)).flatten ++
      personNameMatch(edoc.editors, "editor").getOrElse(Seq()) ++
      personNameMatch(edoc.creators, "author").getOrElse(Seq())
    )
  }

  private def titleMatch(title: Option[String]): Option[MatchQueryDefinition] = {
    title match {
      case Some(t) =>
        Some(MatchQueryDefinition(field = "title", value = t, boost = Some(0.5), fuzziness = Some("10"), operator = Some(Operator.AND)))
      case None =>
        None
    }
  }

  private def personNameMatch(person: Option[List[Person]], parentField: String): Option[Seq[MatchQueryDefinition]] = person match {
    case Some(pers) => Some(pers flatMap (x => {
      (if (x.given.isDefined) Seq(MatchQueryDefinition(field = parentField + ".given", value = x.given.get, boost = Some(0.5), fuzziness = Some("1"), operator = Some(Operator.AND))) else Seq()) ++
        (if (x.family.isDefined) Seq(MatchQueryDefinition(field = parentField + ".family", value = x.family.get, boost = Some(0.5), fuzziness = Some("1"), operator = Some(Operator.AND))) else Seq())
    }))
    case None =>
      None
  }

  private def isbnMatch(title: Option[String]): Option[MatchQueryDefinition] = {
    title match {
      case Some(i) =>
        Some(MatchQueryDefinition(field = "isbn", value = Utilities.isbn10ToIsbn13(i), boost = Some(0.5), fuzziness = Some("10"), operator = Some(Operator.AND)))
      case None =>
        None
    }
  }

  private def issnMatch(title: Option[String]): Option[MatchQueryDefinition] = {
    title match {
      case Some(t) =>
        Some(MatchQueryDefinition(field = "issn", value = t, boost = Some(0.5), fuzziness = Some("10"), operator = Some(Operator.AND)))
      case None =>
        None
    }
  }

}
