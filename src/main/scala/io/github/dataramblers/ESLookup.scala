package io.github.dataramblers

import com.sksamuel.elastic4s.{ElasticsearchClientUri, IndexesAndTypes}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.MultiSearchResponse
import com.sksamuel.elastic4s.searches.{MultiSearchDefinition, SearchDefinition}
import com.sksamuel.elastic4s.searches.queries.matches.MatchQueryDefinition
import org.elasticsearch.index.query.Operator

object ESLookup {

  val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

  def lookup(edoc: Edoc, index: String, doctype: String): Edoc = {
    val sResponse: MultiSearchResponse = client.execute {
      buildQuery(edoc, index, doctype)
    }.await

    (for (res <- sResponse.responses)
      yield (res.maxScore, res.totalHits, res.hits.hits.map(x => x.sourceAsMap).take(1)(0).get("DOI"))).foreach(println)

    // edoc.copy(doi = Some(), score = Some(), records = Some())
  }

  private def buildQuery(edoc: Edoc, index: String, doctype: String): MultiSearchDefinition = {
    MultiSearchDefinition(Seq(issnMatch(edoc.issn, index, doctype), titleMatch(edoc.title, index, doctype), isbnMatch(edoc.isbn, index, doctype)) ++
      personMatch(edoc.creators, "author", index, doctype) ++
      personMatch(edoc.editors, "editor", index, doctype))
  }

  private def titleMatch(title: Option[String], index: String, doctype: String): SearchDefinition = {
    title match {
      case Some(t) =>
        SearchDefinition(IndexesAndTypes(index, doctype), query = Some(MatchQueryDefinition(field = "title", value = t, boost = Some(0.5), fuzziness = Some("10"), operator = Some(Operator.AND))))
      case None =>
        SearchDefinition(IndexesAndTypes(index, doctype), query = None)
    }
  }

  private def personMatch(person: Option[List[Person]], personType: String, index: String, doctype: String): Seq[SearchDefinition] = {
    person match {
      case Some(pers) =>
        pers.flatMap(p =>
          Seq(SearchDefinition(IndexesAndTypes(index, doctype), query = Some(MatchQueryDefinition(field = personType + ".given", value = p.given, boost = Some(0.5), fuzziness = Some("1"), operator = Some(Operator.AND)))),
            SearchDefinition(IndexesAndTypes(index, doctype), query = Some(MatchQueryDefinition(field = personType + ".family", value = p.family, boost = Some(0.5), fuzziness = Some("1"), operator = Some(Operator.AND))))
          )
        )
      case None =>
        Seq(SearchDefinition(IndexesAndTypes(index, doctype), query = None))
    }
  }

  private def isbnMatch(title: Option[String], index: String, doctype: String): SearchDefinition = {
    title match {
      case Some(i) =>
        SearchDefinition(IndexesAndTypes(index, doctype), query = Some(MatchQueryDefinition(field = "isbn", value = Utilities.isbn10ToIsbn13(i), boost = Some(0.5), fuzziness = Some("10"), operator = Some(Operator.AND))))
      case None =>
        SearchDefinition(IndexesAndTypes(index, doctype), query = None)
    }
  }

  private def issnMatch(title: Option[String], index: String, doctype: String): SearchDefinition = {
    title match {
      case Some(t) =>
        SearchDefinition(IndexesAndTypes(index, doctype), query = Some(MatchQueryDefinition(field = "issn", value = t, boost = Some(0.5), fuzziness = Some("10"), operator = Some(Operator.AND))))
      case None =>
        SearchDefinition(IndexesAndTypes(index, doctype), query = None)
    }
  }

}
