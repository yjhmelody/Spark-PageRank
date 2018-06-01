//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.HashPartitioner
//import org.apache.spark.rdd.RDD
//
//def pageRank(links: RDD[(String, List[String])]): Unit = {
//
//  var ranks = links.mapValues(v => 1.0)
//  for (i <- 0 until 10) {
//    val contributions = links.join(ranks).flatMap {
//      case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
//    }
//    ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
//  }
//
//  ranks.sortByKey().collect().foreach(println)
//}
//
//object App {
//  def main(args: Array[String]): Unit = {
//    // 设置配置
//    val conf = new SparkConf()
//      .setAppName("wordCount")
//      .setMaster("local")
//
//    // 设置上下文
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
//
//    val links = sc.parallelize(List(("A", List("B", "C")), ("B", List("A", "C")), ("C", List("A", "B", "D")), ("D", List("C")))).partitionBy(new HashPartitioner(100)).persist()
//
//  }
//}