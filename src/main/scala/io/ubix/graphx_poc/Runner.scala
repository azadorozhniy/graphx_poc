package io.ubix.graphx_poc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{PartitionStrategy, GraphLoader, Graph}
import org.apache.spark.graphx.lib.Analytics
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut


object Runner extends App {

  override def main(args: Array[String]): Unit = {
    val logFile = "/Users/devartemz/Downloads/spark-0.9.0-incubating" // Should be some file on your system

    if (args.length < 2) {
      System.err.println(
        "Usage: LiveJournalPageRank <master> <edge_list_file>\n" +
          "    [--tol=<tolerance>]\n" +
          "        The tolerance allowed at convergence (smaller => more accurate). Default is " +
          "0.001.\n" +
          "    [--output=<output_file>]\n" +
          "        If specified, the file to write the ranks to.\n" +
          "    [--numEPart=<num_edge_partitions>]\n" +
          "        The number of partitions for the graph's edge RDD. Default is 4.\n" +
          "    [--partStrategy=RandomVertexCut | EdgePartition1D | EdgePartition2D | " +
          "CanonicalRandomVertexCut]\n" +
          "        The way edges are assigned to edge partitions. Default is RandomVertexCut.")
      System.exit(-1)
    }


    val host = "spark://rliskovenko-mob.od5.lohika.com:7077"
    val fname = "/Users/devartemz/Downloads/soc-LiveJournal1.txt"

    val tol: Float = 0.001F
    val outFname = "outFile"
    val numEPart = 4


    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
      .set("spark.broadcast.blockSize","1024")

    val sc = new SparkContext(host, "PageRank(" + fname + ")", conf)
    sc.addJar("target/graphx-1.0.jar")
    val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
      minEdgePartitions = numEPart).cache()
    val graph = Some(RandomVertexCut).foldLeft(unpartitionedGraph)(_.partitionBy(_))
    println("GRAPHX: Number of vertices " + graph.vertices.count)
    println("GRAPHX: Number of edges " + graph.edges.count)

    val pr  = graph.pageRank(tol).vertices.cache()
    println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_+_))
    pr.map{case (id, r) =>  id + "\t" + r}.saveAsTextFile(outFname)
   // Analytics.main(args.patch(1, List("pagerank"), 0))
    sc.stop()
  }
}
