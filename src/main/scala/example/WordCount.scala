package example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * WordCount
  *
  * @author chenqixu
  */
object WordCount {
//  private val master = "spark://master75:7077"
  private val master = "spark://master75:7077 yarn"
  private val remote_file = "hdfs://10.1.8.75:8020/test/mrinput/1.data"
//  private val remote_file = "hdfs://10.1.8.75:8020/2.txt"
  private val local_jar = "D:\\Document\\Workspaces\\Git\\TestScala\\out\\artifacts\\TestScala_jar\\TestScala.jar"
  private val local = "local"
  private val local_file = "file:///d:/tmp/data//input//orcinput.data"
  private val appName = "WordCount"

  def localJob(): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster(local)
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(remote_file)
//    //把内容按分隔符切割成1行1行的记录
//    val words = textFile.flatMap(line => line.split("\\|"))
//    //把内容变成map，key是内存，value是1
//    val wordPairs = words.map(word => (word, 1))
//    //聚合map，每找到两个相同的key，就把他们的value相加（a+b）
//    val wordCounts = wordPairs.reduceByKey((a, b) => a + b)
//    println(appName + "：")
//    //循环打印
//    wordCounts.foreach(println)
    //    wordCounts.collect().foreach(println)
    //    wordCounts.collect { case (a, b) => a + b }.foreach(println)
    //打印行数
    println("count：" + textFile.count())
    sc.stop()
  }

  def remoteJob(): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
      .set("spark.executor.memory", "512m").setJars(Seq("./out\\artifacts\\TestScala_jar\\TestScala.jar"))
    System.setProperty("HADOOP_USER_NAME", "edc_base")
    System.setProperty("user.name", "webmaster")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(remote_file)
    val wordCount = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    localJob()
//    remoteJob()
  }
}
