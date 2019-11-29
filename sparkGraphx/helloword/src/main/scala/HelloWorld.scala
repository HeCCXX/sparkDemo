import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")

    val sc = new SparkContext(conf)

    //创建顶点集合，顶点属性是一个二元组
    val users:RDD[(VertexId,(String,String))] = sc.parallelize(Array((3L,("hcx","student")),
      (7L,("hcx","god")),(5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    //创建一个边的集合，边的属性是String属性
    val relationships : RDD[Edge[String]] = sc.parallelize(Array(Edge(3L,7L,"cool"),Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("hgg","Missing")

    //创建一张图，传入顶点和边
    val graph = Graph(users,relationships,defaultUser)

    //过滤图上的所有顶点，如果顶点的第二个值为student，计算满足条件的定点数量
    val verticesCount = graph.vertices.filter{case (id,(name,pos)) => pos == "student"}.count

    println(verticesCount)

    //计算满足条件的边的数量
    val edgeCount = graph.edges.filter(e=>e.srcId > e.dstId).count()
    println(edgeCount)

    sc.stop()
  }

}
