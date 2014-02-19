package io.ubix.graphx_poc

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object TrackFollowers {

  val usersFilePath: String = "data/users.txt"

  val followersFilePath: String = "data/followers.txt"

  def trackFollowers(sc:SparkContext) = {
    val users = sc.textFile(usersFilePath)
      .map(line => line.split(",")).map(parts => (parts.head.toLong, parts.tail))

    // Parse the edge data which is already in userId -> userId format
    val followerGraph = GraphLoader.edgeListFile(sc, followersFilePath)

    // Attach the user attributes
    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      // Some users may not have attributes so we set them as empty
      case (uid, deg, None) => Array.empty[String]
    }

    // Restrict the graph to users with usernames and names
    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

    // Compute the PageRank
    val pagerankGraph = subgraph.pageRank(0.001)

    // Get the attributes of the top pagerank users
    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }

    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
  }

}
