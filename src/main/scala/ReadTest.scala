import java.io._
import scala.io.Source
import scala.util.parsing.json.JSON
import scala.collection.mutable.ArrayBuffer


object ReadTest {
  type Pages = Map[String, List[Map[String, List[String]]]]
  type Links = List[(String, List[String])]

  def readPageLinks(path: String): Unit = {
    val data = Source.fromFile(path).mkString
    val json = JSON.parseFull(data)
    val pageLinks = json.get.asInstanceOf[Pages].get("pageLinks").get
    var urls: Links = List()
    for(page <- pageLinks) {
      val url = page.get("url").get.asInstanceOf[String]
      val links = page.get("links").get.asInstanceOf[List[String]]
      println(url, links)
      urls = (url, links)::urls
    }

    urls
  }

  def main(args: Array[String]): Unit = {
    readPageLinks("src/data/data.json")
  }
}
