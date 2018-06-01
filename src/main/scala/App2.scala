import java.io._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import page._

object App2 {
  def main(args: Array[String]): Unit = {
    // 设置配置
    val conf = new SparkConf()
      .setAppName("wordCount")
      .setMaster("local")
    // 设置上下文
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    // 读取并解析 pagelinks
    val readPath = "data/links10000.json"
    // 设置系数
    val count = 50
    val alpha = 0.15
    val data = readPageLinks(readPath)
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
}
