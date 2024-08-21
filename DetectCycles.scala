import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class DetectCycle(spark: SparkSession, df: DataFrame, idColumn: String, parentIdColumn: String) {

  import spark.implicits._

  def createGraph(): Graph[Int, Int] = {
    val edgesRDD: RDD[Edge[Int]] = df.select(idColumn, parentIdColumn)
      .rdd
      .map(row => (row.getLong(0), row.getLong(1)))
      .map { case (src, dst) => Edge(src, dst, 1) }

    val verticesRDD: RDD[(VertexId, Long)] = df.select(idColumn).distinct()
      .rdd
      .map(row => (row.getLong(0), row.getLong(0)))

    Graph(verticesRDD, edgesRDD)
  }

object GraphCyclicChecker {
  def isCyclic(graph: Graph[_, _]): Boolean = {
    // Compute Strongly Connected Components
    val sccGraph = graph.stronglyConnectedComponents(1)
    
    // Extract the SCCs by collecting their vertices
    val sccs: RDD[(VertexId, VertexId)] = sccGraph.vertices

    // Check if any SCC contains more than one node
    val sccSizes: RDD[Long] = sccs.map { case (_, componentId) =>
      (componentId, 1L)
    }.reduceByKey(_ + _)
    
    // If any SCC has more than one node, then there is a cycle
    val hasCycle = sccSizes.map(_._2).filter(_ > 1).count() > 0

    hasCycle
  }
}
