package io.github.dataramblers

import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient

object ElasticsearchClient {

  private var httpClient: HttpClient = _

  def setup(host: String, port: Int): Unit = {
    ElasticsearchClient.httpClient = HttpClient(ElasticsearchClientUri(host, port))
  }

  def getClient: HttpClient  = {
    ElasticsearchClient.httpClient
  }
}
