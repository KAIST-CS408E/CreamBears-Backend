package services.xis.elastic

import scala.collection.JavaConverters._

import org.apache.http.HttpHost

import org.elasticsearch.client._
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest

import services.xis.crawl.Article

class Indexer(
  host: String, port0: Int, port1: Int, protocol: String,
  index: String, typ: String
) {
  private val analyzer = "openkoreantext-analyzer"

  private val client: RestHighLevelClient =
    new RestHighLevelClient(RestClient.builder(
        new HttpHost(host, port0, protocol),
        new HttpHost(host, port1, protocol)
    ))

  def createIndex(): Unit = {
    val getRequest = new GetIndexRequest().indices(index)
    val exists =
      safe(client.indices.exists(getRequest, RequestOptions.DEFAULT))

    if (!exists) {
      val settings = Settings.builder
        .put("index.number_of_shards", 1)
        .put("index.number_of_replicas", 0)

      val request = new CreateIndexRequest(index)
        .settings(settings)
        .mapping(typ, mapping)

      client.indices.create(request, RequestOptions.DEFAULT)
    }
  }

  def articleExists(id: String): Boolean = {
    val request = new GetRequest(index, typ, id)
    client.exists(request, RequestOptions.DEFAULT)
  }

  def updateHits(id: String, hits: Int): Unit = {
    val request = new UpdateRequest(index, typ, id)
      .doc(JMap("hits" -> hits.toString))
    client.update(request, RequestOptions.DEFAULT)
  }

  def indexArticle(art: Article, att: String, img: String): Unit = {
    import art._
    val request = new IndexRequest(index, typ, id)
      .source(
        "board", board,
        "title", title,
        "author", author,
        "department", department,
        "time", time,
        "hits", hits.toString,
        "content", content,
        "attached", att,
        "image", img
      )
    client.index(request, RequestOptions.DEFAULT)
  }

  def close(): Unit = client.close()

  private def JMap(elems: (String, AnyRef)*) = Map(elems: _*).asJava

  private val mapping = JMap("properties" -> JMap(
    "board" -> JMap(
      "type" -> "keyword"
    ),
    "title" -> JMap(
      "type" -> "text",
      "analyzer" -> analyzer
    ),
    "author" -> JMap(
      "type" -> "keyword"
    ),
    "department" -> JMap(
      "type" -> "keyword"
    ),
    "time" -> JMap(
      "type" -> "date",
      "format" -> "yyyy.MM.dd HH:mm:ss"
    ),
    "hits" -> JMap(
      "type" -> "integer"
    ),
    "content" -> JMap(
      "type" -> "text",
      "analyzer" -> analyzer
    ),
    "attached" -> JMap(
      "type" -> "text",
      "analyzer" -> analyzer
    ),
    "image" -> JMap(
      "type" -> "text",
      "analyzer" -> analyzer
    )
  ))
}
