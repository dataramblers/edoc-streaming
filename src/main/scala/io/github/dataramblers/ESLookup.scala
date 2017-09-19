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
        defaultEmptyVal(hit.sourceFieldOpt("title"))))
    }
  }

  def lookup(edoc: Edoc, index: String, doctype: String): Edoc = {
    val query = search(index / doctype) query buildQuery(edoc, index, doctype)
    client.show(query)
    print(client.show(search(index / doctype) query buildQuery(edoc, index, doctype)))
    val sResponse: SearchResponse = client.execute {
      search(index / doctype) query buildQuery(edoc, index, doctype)
    }.await
    println(sResponse.maxScore)
    val serialisedResult = sResponse.to[Crossref]
    // TODO: Redefine threshold
    if (sResponse.totalHits > 0 && sResponse.maxScore >= 1.1)
      sResponse.hits.hits(0)
      edoc.copy(doi = Some(serialisedResult(0).DOI), score = Some(sResponse.maxScore), results = Some(sResponse.totalHits))
    else
      edoc
  }

  private def buildQuery(edoc: Edoc, index: String, doctype: String): BoolQueryDefinition = {
    BoolQueryDefinition(
      must = Seq(
        titleMatch(edoc.title)).flatten ++
        personNameMatch(edoc.editors, "editor").getOrElse(Seq()) ++
        personNameMatch(edoc.creators, "author").getOrElse(Seq()),
      should = Seq(
        issnMatch(edoc.issn),
        issnMatch(edoc.issn_e),
        isbnMatch(edoc.isbn),
        isbnMatch(edoc.isbn_e)).flatten
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
      // TODO: Find reasonable values for boost and fuzziness
      (if (x.given.isDefined) Seq(MatchQueryDefinition(field = parentField + ".given", value = x.given.get, boost = Some(0.5), fuzziness = Some("10"), operator = Some(Operator.AND))) else Seq()) ++
        (if (x.family.isDefined) Seq(MatchQueryDefinition(field = parentField + ".family", value = x.family.get, boost = Some(0.5), fuzziness = Some("5"), operator = Some(Operator.AND))) else Seq())
    }))
    case None =>
      None
  }

  private def isbnMatch(title: Option[String]): Option[MatchQueryDefinition] = {
    title match {
      case Some(i) =>
        // TODO: Find reasonable values for boost and fuzziness
        Some(MatchQueryDefinition(field = "isbn", value = Utilities.isbn10ToIsbn13(i), boost = Some(0.5), fuzziness = Some("10"), operator = Some(Operator.AND)))
      case None =>
        None
    }
  }

  private def issnMatch(title: Option[String]): Option[MatchQueryDefinition] = {
    title match {
      case Some(t) =>
        // TODO: Find reasonable values for boost and fuzziness
        Some(MatchQueryDefinition(field = "issn", value = t, boost = Some(0.5), fuzziness = Some("10"), operator = Some(Operator.AND)))
      case None =>
        None
    }
  }

}