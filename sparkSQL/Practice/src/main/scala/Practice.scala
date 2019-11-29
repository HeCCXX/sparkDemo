import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class tbStock(ordernumber:String,locationid:String,dateid:String)
case class tbStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double)
case class tbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int)
object Practice {


  def insertHive(spark: SparkSession, tableName: String, toDF: DataFrame): Unit = {
    spark.sql("DROP TABLE IF EXISTS" + tableName)
    toDF.write.saveAsTable(tableName)
  }

  def insertMySQL(tablename: String, result1: DataFrame): Unit = {
    result1.write.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/hcxtest")
      .option("dbtable",tablename)
      .option("user","root")
      .option("password","010251")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Practice").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    val tbStockRDD = spark.sparkContext.textFile("E:\\JavaProject\\sparkDemo\\sparkSQL\\Practice\\src\\main\\resources\\tbStock.txt")
    val tbStockDS = tbStockRDD.map(_.split(",")).map(attr => tbStock(attr(0),attr(1),attr(2))).toDS
//    insertHive(spark,"tbStock",tbStockDS.toDF)

    val result1 = spark.sql("SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount) FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear ORDER BY c.theyear")
    insertMySQL("XQ1",result1)
  }
}
