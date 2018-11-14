package services.xis.elastic

import scala.collection.JavaConverters._

import org.joda.time.format.DateTimeFormat

import org.apache.http.HttpHost

import org.elasticsearch.client._
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest

import services.xis.crawl.ArticleSummary

class Indexer(
  host: String, port0: Int, port1: Int, protocol: String,
  index: String, typ: String,
  startD: String = "20000101000000", endD: String = "20991231235959"
) {
  private val analyzer = "openkoreantext-analyzer"
  private val format = DateTimeFormat.forPattern("yyyy.MM.dd HH:mm:ss")
  private val format1 = DateTimeFormat.forPattern("yyyyMMddHHmmss")

  private val start = format1.parseDateTime(startD)
  private val end = format1.parseDateTime(endD)

  private val client: RestHighLevelClient =
    new RestHighLevelClient(RestClient.builder(
        new HttpHost(host, port0, protocol),
        new HttpHost(host, port1, protocol)
    ))

  def createIndex(): Unit = {
    val getRequest = new GetIndexRequest().indices(index)
    val exists = client.indices.exists(getRequest, RequestOptions.DEFAULT)

    if (!exists) {
      val settings = Settings.builder
        .put("index.number_of_shards", 3)
        .put("index.number_of_replicas", 2)

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

  def updateHits(summ: ArticleSummary): Unit = {
    val request = new UpdateRequest(index, typ, summ.id)
      .doc(JMap("hits" -> summ.hits.toString))
    client.update(request, RequestOptions.DEFAULT)
  }

  def indexArticle(artDoc: ArticleDocument): Unit = {
    import artDoc.article._

    val dateTime = format.parseDateTime(time)

    if ((start isBefore dateTime) && (dateTime isBefore end)) {
      val request = new IndexRequest(index, typ, id)
        .source(
          "board", board,
          "title", title,
          "author", author,
          "department", department,
          "time", dateTime.toDate,
          "hits", hits.toString,
          "content", content,
          "attached", artDoc.attached,
          "image", artDoc.image
        ).timeout(new TimeValue(2 * 60 * 1000))
      client.index(request, RequestOptions.DEFAULT)
    }
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
      "type" -> "date"
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
