import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class AverageSal extends UserDefinedAggregateFunction{

  //输入数据
  override def inputSchema: StructType = StructType(StructField("salary",LongType)::Nil)

  //每一个分区中的共享变量
  override def bufferSchema: StructType = StructType(StructField("sum",LongType) :: StructField("count",IntegerType) :: Nil)

  //表示UDAF的输出类型
  override def dataType: DataType = DoubleType

  //表示如果有相同的输入是否会存在相同的输出，如果是则true
  override def deterministic: Boolean = true

  //初始化每一个分区中的共享数据
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0
  }

  //每一个分区中的每一条数据聚合的时候需要调用该方法
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)

    buffer(1) = buffer.getInt(1) + 1
  }

  //将每一个分区的输出合并形成最后的数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  //给出计算结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getInt(1)
  }
}

object AverageSal{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("udaf").setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val employee = spark.read.json("E:\\JavaProject\\sparkDemo\\sparkSQL\\UDF_UDAF\\src\\main\\resources\\employees.json")

    employee.createOrReplaceTempView("employee")

    spark.udf.register("average",new AverageSal)

    spark.sql("select average(salary) from employee").show()

    spark.stop()
  }
}
