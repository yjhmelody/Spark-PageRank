package com.yjhmelody

import java.time._

import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable.{ListBuffer, Map}
import scala.io.Source
import scala.util.parsing.json.JSON

case class Link(url:String, links: List[String])
case class ALlPages(pageLinks: List[Link])

object page {
  /**
    * 读取 pages json 数据
    *
    * @param path
    * @return
    */
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
    val buf = ListBuffer[(Int, ListBuffer[Int])]()
    var count = 0
    data.foreach {
      case (url, links) => {
        var id: Int = count
        intMap.get(url) match {
          case Some(newId) => id = newId
          case None => {
            count += 1
            intMap += ((url, id))
          }
        }

        val tempBuf = ListBuffer[Int]()
        links.foreach(url => {
          var id: Int = count
          intMap.get(url) match {
            case Some(newId) => id = newId
            case None => {
              count += 1
              intMap += ((url, id))
            }
          }
          tempBuf += id
        })
        buf += ((id, tempBuf))
      }
    }

    val stringMap: Map[Int, String] = Map()
    intMap.foreach {
      case (url, id) => stringMap += ((id, url))
    }

    (buf, stringMap, intMap)
  }

  /**
    * PageRank 算法
    *
    * @param totalLinks url 和 正向链接
    * @param pageRanks  url及其links
    * @param n 迭代次数
    * @param a 规范化因子
    * @return url-rank键值对
    */

  def pageRank2(totalLinks: RDD[(Int, ListBuffer[Int])], pageRanks: RDD[(Int, Double)], n: Int = 100, a: Double = 0.15) = {
    var ranks = pageRanks
    for (i <- 0 until n) {
      val contributions = totalLinks.join(ranks).flatMap {
        // (Int, (ListBuffer[Int], Option[Double]))
        // links中的每个link 只能给该url 投 当前 rank / links.size 个票
        case (url, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      // 对每个url的 rank 值做累计
      ranks = contributions.reduceByKey(_ + _).mapValues(a + (1-a) * _)
    }
    ranks
  }


  type Pages = Map[String, List[Map[String, List[String]]]]
  type Links = List[(String, List[String])]
  /**
    * 获取时间戳
    *
    * @return
    */
  def getTimeStamp() = Instant.now().getNano().toString()

  /**
    * pageRank 算法
    *
    * @param totalLinks url 和 正向链接
    * @param pageRanks  url及其links
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
      ranks = contributions.reduceByKey(_ + _).mapValues(a + (1 - a) * _)
    }
    ranks
  }

  /**
    * 读取 JSON 数据
    *
    * @param path
    * @return 邻接列表
    */
  def readPageLinks(path: String) = {
    val data = Source.fromFile(path).mkString
    val json = JSON.parseFull(data)
    val pageLinks = json.get.asInstanceOf[Pages]("pageLinks")
    var links: Links = List()
    for (page <- pageLinks) {
      val url = page("url").asInstanceOf[String]
      val linksList = page("link").asInstanceOf[List[String]]
      links = (url, linksList)::links
    }
    links
  }
}

