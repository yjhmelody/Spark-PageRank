//import org.apache.spark._
//import org.apache.spark.sql
//
//object App3 {
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
//    // demo1
//    val textFile = sc.textFile("src/data/txco2.csv")
//    val counts = textFile.flatMap(line => line.split(" "))
//      .map(word => (word, 1))
//      .reduceByKey(_ + _)
//
//    counts.foreach(println)
//
//    // demo2
//    val lines = sc.parallelize(Array("hello world", "spark"))
//    lines.foreach(println)
//
//    // demo3
//    counts.mapValues(b => (b, b)).foreach(println)
//    counts.keys.foreach(println)
//    lines.keyBy(value => value.length).foreach(println)
//  }
//}
