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

  def detectCycle(graph: Graph[Int, Int]): Boolean = {
    // Function to check for cycles in a single connected component
    def processComponent(startVertex: VertexId, visited: Set[VertexId]): Boolean = {
      // Initialize the stack with the start vertex and an empty path stack
      val initialStack = List((startVertex, Set(startVertex)))
      // Iteratively process the stack
      val (cycleDetected, _) = initialStack.foldLeft((false, visited, List[(VertexId, Set[VertexId])]())) {
        case ((foundCycle, currentVisited, stack), (vertexId, pathStack)) =>
          if (foundCycle) {
            (true, currentVisited, stack)
          } else if (pathStack.contains(vertexId)) {
            (true, currentVisited, stack)
          } else if (currentVisited.contains(vertexId)) {
            (foundCycle, currentVisited, stack)
          } else {
            val newVisited = currentVisited + vertexId
            val neighbors = graph.edges.filter(e => e.srcId == vertexId).map(e => e.dstId).collect()
            val newStack = neighbors.map(neighbor => (neighbor, pathStack + neighbor)).toList
            (foundCycle, newVisited, newStack ++ stack)
          }
      }
      cycleDetected
    }

    // Collect all vertices and process each component
    val vertices = graph.vertices.map(_._1).collect()
    val initialState = (false, Set[VertexId]())
    val finalState = vertices.foldLeft(initialState) { case ((foundCycle, visited), vertex) =>
      if (foundCycle) {
        (true, visited)
      } else if (visited.contains(vertex)) {
        (foundCycle, visited)
      } else {
        val cycleFound = processComponent(vertex, visited)
        (foundCycle || cycleFound, visited + vertex)
      }
    }
    finalState._1
  }
}
