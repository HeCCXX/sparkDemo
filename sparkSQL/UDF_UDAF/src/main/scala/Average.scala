import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

case class Employee(name:String,salary:Long)
case class Aver(var sum: Long,var count:Int)
class Average extends Aggregator[Employee,Aver,Double]{
  //初始化方法，初始化每一个分区中的共享变量
  override def zero: Aver = Aver(0L,0)

  //每一个分区中的每一条数据聚合的时候需要调用该方法
  override def reduce(b: Aver, a: Employee): Aver = {
    b.sum = b.sum + a.salary
    b.count = b.count + 1
    b
  }

  //每一个分区的输出，合并，形成最后的数据
  override def merge(b1: Aver, b2: Aver): Aver = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  //给出计算结果
  override def finish(reduction: Aver): Double = {
    reduction.sum.toDouble / reduction.count
  }

  //主要用于对共享变量进行编码
  override def bufferEncoder: Encoder[Aver] = Encoders.product

  //主要用于将输出进行编码
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
object Average{
  def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("udaf")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    val employee = spark.read.json("E:\\JavaProject\\sparkDemo\\sparkSQL\\UDF_UDAF\\src\\main\\resources\\employees.json").as[Employee]
    val aver = new Average().toColumn.name("average")

    employee.select(aver).show()

    spark.stop()
  }
}
