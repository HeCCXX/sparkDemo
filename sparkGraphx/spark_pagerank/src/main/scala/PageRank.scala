import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {

  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local[*]").setAppName("pagerank")

    val sc = new SparkContext(conf)

    val erdd = sc.textFile("E:\\JavaProject\\sparkDemo\\sparkGraphx\\spark_pagerank\\src\\main\\resources\\graphx-wiki-edges.txt")

    val edges = erdd.map(x => {val para = x.split("\t");Edge(para(0).trim.toLong,para(1).trim.toLong,0)})

    val vrdd = sc.textFile("E:\\JavaProject\\sparkDemo\\sparkGraphx\\spark_pagerank\\src\\main\\resources\\graphx-wiki-vertices.txt")
    val vertices = vrdd.map(x => {val para =  x.split("\t");(para(0).trim.toLong,para(1).trim)})

    val graph = Graph(vertices,edges)

    val prGraph = graph.pageRank(0.001).cache()

    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
      (v,title,rank) => (rank.getOrElse(0.0),title)
    }
    titleAndPrGraph.vertices.top(10){
      Ordering.by((entry :(VertexId,(Double,String))) => entry._2._1)
    }.foreach(t => println(t._2._2 + " : "+ t._2._1))

  sc.stop()
  }

}
