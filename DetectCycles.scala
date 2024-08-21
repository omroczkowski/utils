import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class DetectCycle(spark: SparkSession, df: DataFrame, idColumn: String, parentIdColumn: String) {

  // Create a Graph from a DataFrame with parameterized id and parent_id columns
  def createGraphFromDataFrame(df: DataFrame, idCol: String, parentIdCol: String): Graph[Int, Int] = {
    // Convert DataFrame to RDD[VertexId] and RDD[Edge[Int]]
    
    // Extract vertices
    val vertexRDD: RDD[(VertexId, Int)] = df.select(idCol).distinct().rdd.map { row =>
      val id = row.getAs[Int](idCol).toLong
      (id, 0) // 0 is a placeholder for vertex attribute
    }
    
    // Extract edges
    val edgeRDD: RDD[Edge[Int]] = df.select(idCol, parentIdCol).rdd.map { row =>
      val srcId = row.getAs[Int](parentIdCol).toLong
      val dstId = row.getAs[Int](idCol).toLong
      Edge(srcId, dstId, 1) // 1 is a placeholder for edge attribute
    }
    
    // Create the graph
    Graph(vertexRDD, edgeRDD)
  }

  // Check if the graph is cyclic
def isCyclic(graph: Graph[Int, Int]): Boolean = {
  // Compute Strongly Connected Components
  val sccGraph: Graph[VertexId, Int] = graph.stronglyConnectedComponents(Int.MaxValue)

  // Extract the SCCs by collecting their vertices
  val sccs: RDD[(VertexId, VertexId)] = sccGraph.vertices

  // Count the size of each SCC
  val sccSizes: RDD[(VertexId, Long)] = sccs.map { case (_, componentId) =>
    (componentId, 1L)
  }.reduceByKey(_ + _)

  // If any SCC has more than one node, then there is a cycle
  val hasCycle: Boolean = sccSizes.filter(_._2 > 1).count() > 0

  hasCycle
}
}
}
