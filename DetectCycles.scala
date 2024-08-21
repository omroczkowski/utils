import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

class DetectCycle(spark: SparkSession) {

  import spark.implicits._

/**
    * Method to create a directed graph from a DataFrame with specified 'id' and 'parent_id' columns.
    *
    * @param df DataFrame containing 'id' and 'parent_id' columns.
    * @param idCol Name of the column representing node IDs.
    * @param parentCol Name of the column representing parent node IDs.
    * @return A directed graph.
    */
  def createGraph(df: DataFrame, idCol: String, parentCol: String): Graph[Int, Int] = {
    // Create an RDD of edges for the directed graph
    val edges: RDD[Edge[Int]] = df
      .filter(col(parentCol) =!= 0)
      .rdd
      .map(row => Edge(row.getAs[Int](parentCol).toLong, row.getAs[Int](idCol).toLong, 1))

    // Create an RDD of vertices
    val vertices: RDD[(VertexId, Int)] = df
      .rdd
      .flatMap(row => Seq(
        (row.getAs[Int](idCol).toLong, row.getAs[Int](idCol)),
        (row.getAs[Int](parentCol).toLong, row.getAs[Int](parentCol))
      ))
      .distinct()

    // Create the directed graph
    Graph(vertices, edges)
  }

  /**
    * Method to detect if there is a cycle in the directed graph using DFS.
    *
    * @param graph A directed graph.
    * @return True if the graph contains a cycle, False otherwise.
    */
def hasCycle(graph: Graph[Int, Int]): Boolean = {
  // Define a mutable visited set that persists across DFS invocations
  var visited = Set[VertexId]()
  
  // Define a function to perform DFS and check for cycles
  def dfs(vertexId: VertexId, stack: Set[VertexId]): Boolean = {
    if (stack.contains(vertexId)) {
      // A cycle is detected if the current vertex is already in the stack
      return true
    }

    if (visited.contains(vertexId)) {
      // If the vertex is already visited, no need to process it again
      return false
    }

    // Mark the current node as visited and add it to the stack
    visited += vertexId
    val newStack = stack + vertexId

    // Explore all neighbors of the current node
    val neighbors = graph.edges
      .filter(e => e.srcId == vertexId)
      .map(e => e.dstId)
      .collect()

    // Recursively perform DFS on neighbors
    for (neighbor <- neighbors) {
      if (dfs(neighbor, newStack)) {
        return true // Cycle detected in the recursive call
      }
    }

    // No cycle detected from this node
    false
  }

  // Check all vertices for cycles
  graph.vertices.map(_._1).collect().exists(v => dfs(v, Set()))
}

  /**
    * Method that combines graph creation and cycle detection.
    *
    * @param df DataFrame containing 'id' and 'parent_id' columns.
    * @param idCol Name of the column representing node IDs.
    * @param parentCol Name of the column representing parent node IDs.
    * @return True if the graph contains a cycle, False otherwise.
    */
  def detectCycle(df: DataFrame, idCol: String, parentCol: String): Boolean = {
    val graph = createGraph(df, idCol, parentCol)
    hasCycle(graph)
  }
}
