package services.xis.elastic

import services.xis.extractor.Extractor
import services.xis.crawl.Article

class ArticleDocument(val article: Article) {
  private var _attached = ""
  private var _image = ""
  private var obtained = 0

  def image: String = _image
  def attached: String = _attached

  def add(meta: FileMeta, str: String): Unit = {
    if (meta.attached) _attached += str
    else _image += str
    obtained += 1
  }

  val files: List[FileMeta] = {
    val att =
      (article.files zip article.links).map{
        case (file, link) =>
          val name =
            if (file.endsWith(" KB)") || file.endsWith(" MB)"))
              file.substring(0, file.lastIndexOf(" ("))
            else file
          FileMeta(name, link, true)
      }
    val img =
      article.images.map(i => FileMeta(i.replace("_", "."), i, false))
    (att ++ img).filter(meta => Extractor.available(meta.name))
  }

  def complete: Boolean = 
    files.length == obtained
}

case class FileMeta(name: String, link: String, attached: Boolean)
