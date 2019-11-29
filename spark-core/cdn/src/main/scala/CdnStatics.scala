import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

object CdnStatics {
  val logger = LoggerFactory.getLogger(CdnStatics.getClass)

  //匹配IP地址
  val IPPattern = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r

  //匹配视频文件名
  val videoPattern = "([0-9]+).mp4".r

  //[15/Feb/2017:09:49:36 +0800] 匹配2017:09  按每小时播放量统计
  val timePattern = ".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r

  //匹配http响应码和请求数据大小
  val httpSizePattern = ".*\\s(200|206|304)\\s([0-9]+)\\s.*".r

  //统计独立IP访问量前10位
  def ipStatics(input: RDD[String]): Unit = {
    val ipNums = input.map(x => (IPPattern.findFirstIn(x).get,1)).reduceByKey(_ +_).sortBy(_._2,false)

    ipNums.take(10).foreach(println)

    println("独立IP数："+ipNums.count())
  }

  def videoIpStatisc(input: RDD[String]): Unit = {
    def getFileNameAndIp(line:String)= {
      (videoPattern.findFirstIn(line).mkString,IPPattern.findFirstIn(line).mkString)
    }
    input.filter(x => x.matches(".*([0-9]+)\\.mp4.*")).map(x => getFileNameAndIp(x)).groupByKey().map(x => (x._1,x._2.toList.distinct)).
      sortBy(_._2.size,false).take(10).foreach(x => println("视频："+ x._1 + "独立IP数：" + x._2.size))
  }



  def flowOfHour(input: RDD[String]): Unit = {
    def isMatch(httpSizePattern: Regex, x: String): Boolean = {
      x match {
        case httpSizePattern(_*) => true
        case _ =>false
      }
    }
    //获取日志中小时和http请求体大小
    def getTimeAndSize(str: String)={
      var res = ("",0L)
      try{
        val httpSizePattern(code,size) = str
        val timePattern(year,hour) = str
        res = (hour,size.toLong)
      }catch {
        case ex : Exception => ex.printStackTrace()
      }
      res
    }
    input.filter(x => isMatch(httpSizePattern,x)).map(x=>getTimeAndSize(x)).groupByKey()
      .map(x=>(x._1,x._2.sum)).sortByKey().foreach(x => println(x._1 + "时 CDN流量 = "+ x._2/(1024*1024*1024)+"G"))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CdnStatics")

    val sc = new SparkContext(conf)

    val input = sc.textFile("E:\\JavaProject\\sparkDemo\\spark-core\\cdn\\src\\main\\resources\\cdn.txt")

    //统计独立IP访问量前10位
    ipStatics(input)

    //统计每个视频独立IP数
    videoIpStatisc(input)

    //统计一天中每个小时间的流量
    flowOfHour(input)
  }
}
