package io.ubix.graphx_poc

import org.apache.spark._


object Runner extends App {

  class VertexProperty()
  case class Relation(from: String, to: String) extends VertexProperty

  def pageHash(title: String) = title.toLowerCase().replace(" ","").hashCode

  override def main(args: Array[String]): Unit = {
    val host = "spark://rliskovenko-mob.od5.lohika.com:7077"

    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
    conf.set("spark.broadcast.blockSize","1024")

    val sc = new SparkContext(host, "GraphX playground", conf)
    sc.addJar("target/graphx-1.0.jar")

    TrackFollowers.trackFollowers(sc)
    sc.stop()
  }
}
