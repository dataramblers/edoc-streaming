import com.sksamuel.elastic4s.{ElasticsearchClientUri, IndexesAndTypes}
import com.sksamuel.elastic4s.searches.{MultiSearchDefinition, SearchDefinition}
import com.sksamuel.elastic4s.searches.queries.matches.MatchQueryDefinition
import org.elasticsearch.index.query.Operator
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.{MultiSearchResponse, SearchResponse}
import com.sksamuel.elastic4s.searches.queries.{BoolQueryDefinition, QueryDefinition}

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
                eprintid: Integer)

val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

def lookup(edoc: Edoc, index: String, doctype: String): SearchResponse = {
  client.execute {
    search(index/doctype) query buildQuery(edoc, index, doctype)
  }.await
}

def buildQuery(edoc: Edoc, index: String, doctype: String): BoolQueryDefinition = {
  BoolQueryDefinition(must = Seq(titleMatch(edoc.title, index, doctype).get) ++
    personMatch(edoc.creators, "author.given", index, doctype).get)
  //personMatch(edoc.editors, "editor", index, doctype).get)
}


//author given family
// editor given family
// title
// issn
// isbn

def titleMatch(title: Option[String], index: String, doctype: String): Option[QueryDefinition] = {
  title match {
    case Some(t) =>
      Some(MatchQueryDefinition(field = "title", value = t, boost = Some(0.5), fuzziness = Some("10"), operator = Some(Operator.AND)))
    case None =>
      None
  }
}

def personMatch(person: Option[List[Person]], partOfName: String, index: String, doctype: String): Option[Seq[QueryDefinition]] = {
  person match {
    case Some(pers: List[Person]) =>
      Some(pers.map(p =>
        MatchQueryDefinition(field = partOfName, value = p.given, boost = Some(0.5), fuzziness = Some("1"), operator = Some(Operator.AND))))
    case None =>
      None
  }
}

val dummy = Edoc(Some(List(Person("Temchin", "A. N."))), Some(List(Person("", ""))), Some("Some properties of the distribution function of Maxwell gas particles"), None, None, None, None, None, None, 23423)

val test = lookup(dummy, "crossref", "crossref")

//test.hits.hits.foreach(x => println(x.score))

test.totalHits

//for (x <- test.responses) {
//  x.hits.hits.foreach(x => println("Score: " + x.sourceAsString))
//}