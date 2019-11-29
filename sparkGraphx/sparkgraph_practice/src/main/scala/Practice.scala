import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Practice extends App {

  val conf = new SparkConf().setAppName("Graphx").setMaster("local[*]")

  val sc = new SparkContext(conf)

  //初始化顶点集合
  val vertexArray = Array(
    (1L, ("Alice", 28)),
    (2L, ("Bob", 27)),
    (3L, ("Charlie", 65)),
    (4L, ("David", 42)),
    (5L, ("Ed", 55)),
    (6L, ("Fran", 50))
  )
  //创建顶点的RDD
  val vertexRDD :RDD[(Long,(String,Int))] = sc.parallelize(vertexArray)

  //初始化边的集合
  val edgeArray = Array(
    Edge(2L, 1L, 7),
    Edge(2L, 4L, 2),
    Edge(3L, 2L, 4),
    Edge(3L, 6L, 3),
    Edge(4L, 1L, 1),
    Edge(2L, 5L, 2),
    Edge(5L, 3L, 8),
    Edge(5L, 6L, 3)
  )
  //创建边的RDD表示
  val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
  //创建一个图
  val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
  println("属性演示")
  println("**********************************************************")
  println("找出图中年龄大于30的顶点：")

  graph.vertices.filter{case (id,(name,age)) => age > 30}.collect.foreach{
    case (id,(name,age)) => println(s"${name} is ${age}")
  }

  //triplets操作，((srcId, srcAttr), (dstId, dstAttr), attr)
  println("列出边属性>5的tripltes：")
  for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
    println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
  }

  //***************************  转换操作    ****************************************
  println("转换操作")
  println("**********************************************************")
  println("顶点的转换操作，顶点age + 10：")
  graph.mapVertices { case (id, (name, age)) => (id, (name, age + 10)) }.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
  println
  println("边的转换操作，边的属性*2：")
  graph.mapEdges(e => e.attr * 2).edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
  println
  println("三元组的转换操作，边的属性为端点的age相加：")
  graph.mapTriplets(tri => tri.srcAttr._2 + tri.dstAttr._2).triplets.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
  println

  //***************************  结构操作    ****************************************
  println("结构操作")
  println("**********************************************************")
  println("顶点年纪>30的子图：")
  val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)
  println("子图所有顶点：")
  subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
  println
  println("子图所有边：")
  subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
  println
  println("反转整个图：")
  val reverseGraph = graph.reverse
  println("子图所有顶点：")
  reverseGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
  println
  println("子图所有边：")
  reverseGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
  println

  //**************************************连接操作*********************
  case class User(name:String,age:Int,inDeg:Int,outDeg:Int)

  val initialUserGraph : Graph[User,Int] = graph.mapVertices{case (id,(name,age)) => User(name,age,0,0)}

  val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
    case (id,user,indeg) => User(user.name,user.age,indeg.getOrElse(0),user.outDeg)
  }.outerJoinVertices(initialUserGraph.outDegrees){
    case (id,user,outdeg) => User(user.name,user.age,user.inDeg,outdeg.getOrElse(0))
  }
  println("连接图的属性：")
  userGraph.vertices.collect.foreach(v => println(s"${v._2.name} indeg : ${v._2.inDeg} outdeg : ${v._2.outDeg}"))
  println()

  println("初度和入度相同的人员：")
  userGraph.vertices.filter{
    case (id,user: User) => user.inDeg == user.outDeg
  }.collect.foreach{
    case (id,user: User) => println(user.name)
  }

  //***************************  聚合操作    ****************************************
  println("聚合操作")
  println("**********************************************************")
  println("collectNeighbors：获取当前节点source节点的id和属性")
  graph.collectNeighbors(EdgeDirection.In).collect.foreach{
    v => {
      println(s"id: ${v._1}"); for (arr <- v._2){
        println(s"   ${arr._1}   (name : ${arr._2._1}  age: ${arr._2._2})")
      }
    }
  }

  println("aggregateMessages版本：")
  graph.aggregateMessages[Array[(VertexId,(String,Int))]](ctx => ctx.sendToDst(Array((ctx.srcId,(ctx.srcAttr._1,ctx.srcAttr._2)))),_++_)
    .collect.foreach(v => {
    println(s" id:   ${v._1}"); for (arr <- v._2){
      println(s"   ${arr._1}  (name:  ${arr._2._1}  age: ${arr._2._2})  ")
    }
  })

  println("聚合操作")
  println("**********************************************************")
  println("找出年纪最大的追求者：")

  val oldestFollower : VertexRDD[(String,Int)] = userGraph.aggregateMessages(ctx => ctx.sendToDst((ctx.srcAttr.name,ctx.srcAttr.age)),
    (a,b) => if(a._2 > b._2) a else b)

  userGraph.vertices.leftJoin(oldestFollower) {
    (id,user,optOldFollower) => optOldFollower match {
      case  None  => s"${user.name} does have any follower."
      case Some((name,age)) => s"${name} is the oldest follower of ${user.name}"
    }
  }.collect.foreach{case (id,str) => println(str)}

  //***************************  实用操作    ****************************************
  println("聚合操作")
  println("**********************************************************")
  //定义源点
  val sourceId :VertexId = 5L
  val initialGraph = graph.mapVertices((id,_) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

  initialGraph.triplets.collect.foreach(println)

  println("找出5到各顶点的最短距离：")
  val sssp = initialGraph.pregel(Double.PositiveInfinity,Int.MaxValue,EdgeDirection.Out)(
    (id,dist,newDist) => {
      println("||||" + id);math.min(dist,newDist)
    },
    triplet => {  //计算权重
      println(">>>>>" + triplet.srcId)
      if (triplet.srcId + triplet.attr < triplet.dstAttr){
        //send message
        Iterator((triplet.dstId,triplet.srcAttr + triplet.attr))
      }else{
        //发送不成功
        Iterator.empty
      }
    },(a,b) => math.min(a,b)  //当前节点所有输入的最短距离
  )
  sssp.triplets.collect.foreach(println)

  println(sssp.vertices.collect.mkString("\n"))

  sc.stop()

}
