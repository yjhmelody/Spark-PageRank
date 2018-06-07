import java.io._

import org.apache.spark._
import page._

object App2 {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    run2()
    val end = System.currentTimeMillis()
    println("耗时:" + (end - start))
  }

  private def run1() = {
    // 读取并解析 pagelinks
    val readPath = "data/data.json"
    // 设置系数
    val count = 10
    val alpha = 0.15
    val data = readAllPages(readPath)

    // 设置配置
    val conf = new SparkConf()
      .setAppName("pageRank")
      .setMaster("local[4]")
    // 设置上下文
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // 并行化
    val links = sc.parallelize(data)
    // 初始化ranks
    val ranks = links.mapValues(v => 1.0)
    // 迭代
    val newRanks = pageRank(links, ranks, count, alpha)
    val sortedRanks = newRanks.sortBy(_._2).collect()
    // 存储排名
    val writepath = "data/rank-" + sortedRanks.length + "-"  + count.toString() + "-" + getTimeStamp() + ".txt"
    val file = new File(writepath)
    if (!file.exists()) {
      file.createNewFile()
    }
    val writer = new BufferedWriter(new FileWriter(file))

    // 存储和打印
    sortedRanks.foreach(rank => {
      writer.write(rank._1.toString() + " " + rank._2.toString())
      writer.newLine()
      writer.flush()
      println(rank.toString())
    })

    println(writepath)
  }

  private def run2() = {
    // 读取并解析 pagelinks
    val readPath = "data/links10000.json"
    // 设置系数
    val count = 100
    val alpha = 0.15
    val data = readAllPages(readPath)
    // string to int
    val (data2, stringMap, intMap) = convertToInt(data)
    val pageNum = intMap.size

    // 设置配置
    val conf = new SparkConf()
      .setAppName("pageRank")
      .setMaster("local[4]")
    // 设置上下文
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // 并行化
    val links = sc.parallelize(data2)
    // 初始化ranks
    val ranks = links.mapValues(v => 1.0)
    // 迭代
    val newRanks = pageRank2(links, ranks, count, alpha)
    val sortedRanks = newRanks.sortBy(_._2).collect()

    val finalRanks = sortedRanks.map {
      case (id, rank) => (stringMap(id), rank)
    }
    // 存储排名
    val writepath = "data/rank-" + finalRanks.length + "-"  + count.toString() + "-" + getTimeStamp() + ".txt"
    val file = new File(writepath)
    if (!file.exists()) {
      file.createNewFile()
    }
    val writer = new BufferedWriter(new FileWriter(file))

    // 存储和打印
    finalRanks.foreach(rank => {
      writer.write(rank._1.toString() + " " + rank._2.toString())
      writer.newLine()
      writer.flush()
      println(rank.toString())
    })

    println(writepath)
  }
}
