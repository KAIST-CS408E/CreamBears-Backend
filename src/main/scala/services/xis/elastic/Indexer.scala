package services.xis.elastic

import services.xis.crawl.Article

import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client._

object Indexer {

  def index(art: Article): Unit = {
    val client = new RestHighLevelClient(
      RestClient.builder(
        new HttpHost("localhost", 9200, "http"),
        new HttpHost("localhost", 9300, "http")
    ))
    val request = new IndexRequest("posts", "doc", "1")
      .source("user", "kimchy",
              "postDate", "2018-01-30",
              "message", "trying out Elasticsearch");
    val response = client.index(request, RequestOptions.DEFAULT)
    println(response)
    client.close
  }
}
