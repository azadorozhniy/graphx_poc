package io.ubix.graphx_poc


import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut

object CalculatePageRank {

  val sourceFile: String = "/Users/devartemz/Downloads/soc-LiveJournal1.txt"

  val tolerance: Float = 0.001F

  def calculatePageRank(sc: SparkContext) = {
    val outFname = "target/result"
    val numEPart = 4

    val unpartitionedGraph = GraphLoader.edgeListFile(sc, sourceFile, minEdgePartitions = numEPart).cache()
    val graph = Some(RandomVertexCut).foldLeft(unpartitionedGraph)(_.partitionBy(_))

    println("GRAPHX: Number of vertices " + graph.vertices.count)
    println("GRAPHX: Number of edges " + graph.edges.count)

    val pr  = graph.pageRank(tolerance).vertices.cache()

    println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_+_))
    pr.map{case (id, r) =>  id + "\t" + r}.saveAsTextFile(outFname)
  }

}
