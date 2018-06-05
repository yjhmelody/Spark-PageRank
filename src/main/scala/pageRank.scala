import org.apache.spark.rdd.RDD
import java.io._
import java.time._
import scala.collection.mutable.{ListBuffer, Map, ArrayBuffer}

import scala.io.Source
import scala.util.parsing.json.JSON

import org.json4s._
import org.json4s.native.JsonMethods._


case class Link(url:String, links: List[String])
case class ALlPages(pageLinks: List[Link])

object page {
  def main(args: Array[String]): Unit = {
    implicit val formats = DefaultFormats
    val data = readAllPages("data/data.json")
    val (data2, stringMap, intMap) = convertToInt(data)
    intMap.foreach(println)
    println(intMap.size)
    println(data2)
  }

  def readAllPages(path: String) = {
    val data = Source.fromFile(path).mkString

    implicit val formats = DefaultFormats
    val pages = parse(data).extract[ALlPages]
    val buf = ListBuffer[(String, List[String])]()

    pages.pageLinks.foreach {
      case Link(url, links) => buf += ((url, links))
    }

    println("页面数量为" + buf.length)
    buf.toList
  }

  /**
    *
    * @param data 链接
    * @return id 和 url 相互之间的映射
    */
  def convertToInt(data: List[(String, List[String])]) = {
    val intMap: Map[String, Int] = Map()
    val stringMap: Map[Int, String] = Map()
    val buf = ListBuffer[(Int, ListBuffer[Int])]()
    var count = 0
    data.foreach {
      case (url, links) => {
        var id: Int = count
        if(intMap.contains(url)) {
           id = intMap.get(url).get
        } else {
          count += 1
        }
        // 添加新的映射
        intMap += ((url, id))
        stringMap += ((id, url))

        val tempBuf = ListBuffer[Int]()
        links.foreach(link => {
          var id: Int = count
          if(intMap.contains(link)) {
            id = intMap.get(link).get
          }else {
            count += 1
          }
          intMap += ((link, id))
          stringMap += ((id, link))
          tempBuf += id
        })
        buf += ((id, tempBuf))
      }
    }
    (buf, stringMap, intMap)
  }

  /**
    *
    * @param totalLinks url 和 正向链接
    * @param pageRanks
    * @param n 迭代次数
    * @param a 规范化因子
    * @return url-rank键值对
    */
  def pageRank2(totalLinks: RDD[(Int, ListBuffer[Int])], pageRanks: RDD[(Int, Double)], n: Int = 100, a: Double = 0.15) = {
    var ranks = pageRanks
    for (i <- 0 until n) {
      val contributions = totalLinks.join(ranks).flatMap {
        // (Int, (ListBuffer[Int], Double)
        // links中的每个link 只能给该url 投 当前 rank / links.size 个票
        case (url, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      // 累计 rank 值
      ranks = contributions.reduceByKey(_ + _).mapValues(a + (1-a) * _)
    }
    ranks
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
    * @param totalLinks url 和 正向链接
    * @param pageRanks
    * @param n 迭代次数
    * @param a 规范化因子
    * @return url-rank键值对
    */
  def pageRank(totalLinks: RDD[(String, List[String])], pageRanks: RDD[(String, Double)], n: Int = 100, a: Double = 0.15) = {
    var ranks = pageRanks
      for (i <- 0 until n) {
        val contributions = totalLinks.join(ranks).flatMap {
          // (String, (ListBuffer[String], Double)
          // links中的每个link 只能给该url 投 当前 rank / links.size 个票
          case (url, (links, rank)) => links.map(dest => (dest, rank / links.size))
        }
        // 累计 rank 值
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

