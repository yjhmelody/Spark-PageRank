import org.apache.spark.rdd.RDD
import java.io._

import scala.io.Source
import scala.util.parsing.json.JSON

import java.time._

object page {
  def main(args: Array[String]): Unit = {
    val readPath = "data/links.json"
    val data = readPageLinks(readPath)
    println(data)
  }

  type Pages = Map[String, List[Map[String, List[String]]]]
  type Links = List[(String, List[String])]
  /**
    *
    * @return 时间戳
    */
  def getTimeStamp() = Instant.now().getNano().toString()

  /**
    *
    * @param links url 和 正向链接
    * @param pageRanks
    * @param n 迭代次数
    * @param a 规范化因子
    * @return url-rank键值对
    */
  def pageRank(links: RDD[(String, List[String])], pageRanks: RDD[(String, Double)], n: Int = 100, a: Double = 0.15) = {
    var ranks = pageRanks
//    val delta = 1 / links.count()
//    var deltas = pageRanks.clone().asInstanceOf[RDD[(String, Double)]]
      for (i <- 0 until n) {
        val contributions = links.join(ranks).flatMap {
          case (url, (links, rank)) => links.map(dest => (dest, rank / links.size))
        }
//        val oldRanks = ranks
        ranks = contributions.reduceByKey(_ + _).mapValues(a + (1-a) * _)
    }
    ranks
  }

  /**
    *
    * @param path
    * @return 邻接列表
    */
  def readPageLinks(path: String) = {
    val data = Source.fromFile(path).mkString
    val json = JSON.parseFull(data)
    val pageLinks = json.get.asInstanceOf[Pages].get("pageLinks").get
    var links: Links = List()
    for(page <- pageLinks) {
      val url = page.get("url").get.asInstanceOf[String]
      val linksList = page.get("links").get.asInstanceOf[List[String]]
      links = (url, linksList)::links
    }
    links
  }
}

