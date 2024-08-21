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
      .filter(col(parentCol) =!= 0)  // Filter out rows where parent_id is 0 (if 0 indicates no parent)
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
    * Method to detect if there is a cycle in the directed graph.
    *
    * @param graph A directed graph.
    * @return True if the graph contains a cycle, False otherwise.
    */
  def hasCycle(graph: Graph[Int, Int]): Boolean = {
    val initialGraph = graph.mapVertices((id, _) => -1)

    val result = initialGraph.pregel(Int.MaxValue, activeDirection = EdgeDirection.Out)(
      (id, oldDist, newDist) => math.min(oldDist, newDist),  // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr == -1) {
          Iterator((triplet.dstId, triplet.srcId.toInt))
        } else if (triplet.srcAttr == triplet.dstId.toInt) {
          // A cycle is detected
          Iterator((triplet.dstId, -2))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)  // Merge Message
    )

    result.vertices.filter { case (_, dist) => dist == -2 }.count() > 0
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
