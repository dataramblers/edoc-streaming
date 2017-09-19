package io.github.dataramblers

import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.searches.queries.BoolQueryDefinition
import com.sksamuel.elastic4s.searches.queries.matches.MatchQueryDefinition
import org.elasticsearch.index.query.Operator

object ESLookup {

  val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

  def lookup(edoc: Edoc, index: String, doctype: String): Edoc = {
    val sResponse: SearchResponse = client.execute {
      buildQuery(edoc, index, doctype)
    }.await

    //    (for (res <- sResponse.responses)
    //      yield (res.maxScore, res.totalHits, res.hits.hits.map(x => x.sourceAsMap).take(1)(0).get("DOI"))).foreach(println)
    // val serialisedResult = sResponse.safeTo
    //  serialisedResult.

    // TODO: Read out DOI correctly
    if (sResponse.maxScore > 10 /* && sResponse.hits.hits.take(1) > 0 */ )
      edoc.copy(doi = Some("wef"), score = Some(sResponse.maxScore), results = Some(sResponse.totalHits))
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
      personNameMatch(edoc.editors).flatten ++
      personNameMatch(edoc.creators).flatten
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

  // TODO: Implement a function generating the following `Seq` according to the DRY principle
  private def personNameMatch(person: Option[List[Person]]): Option[Seq[MatchQueryDefinition]] = person match {
    case Some(pers) => pers match {
      case p: List[Author] => Some(p flatMap (x => Seq(MatchQueryDefinition(field = "author.given", value = x.given, boost = Some(0.5), fuzziness = Some("1"), operator = Some(Operator.AND)),
        MatchQueryDefinition(field = "author.family", value = x.family, boost = Some(0.5), fuzziness = Some("1"), operator = Some(Operator.AND)))))
      case p: List[Editor] => Some(p flatMap (x => Seq(MatchQueryDefinition(field = "editor.given", value = x.given, boost = Some(0.5), fuzziness = Some("1"), operator = Some(Operator.AND)),
        MatchQueryDefinition(field = "editor.family", value = x.family, boost = Some(0.5), fuzziness = Some("1"), operator = Some(Operator.AND)))))
    }
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
