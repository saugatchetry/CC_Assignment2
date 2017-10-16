import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.xml.{ XML, NodeSeq }

object PageRankGraphX {
  def main(args: Array[String]) {

    val inputFile=args(0)
		val outputFile=args(1)
    val iterations = if (args.length > 2) args(2).toInt else 10

    val sparkConfig = new SparkConf()

    sparkConfig.setAppName("prGraphX")

    sparkConfig.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val context = new SparkContext(sparkConfig)

    val input = context.textFile(inputFile)

    def pageToID(title: String): VertexId = {
      title.toLowerCase.replace(" ", "").hashCode.toLong
    }


    var wiki = input.map(line => {
      val cols = line.split("\t")
      val (wkTitle, ngData) = (cols(0), cols(3).replace("\\n", "\n"))

      val extracted_links =
        if (ngData == "\\N") {
          NodeSeq.Empty
        } else {
          try {
            XML.loadString(ngData) \\ "link" \ "target"

          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.println("Error")
              NodeSeq.Empty
          }
        }

      val outLinks = extracted_links.map(link => new String(link.text)).toArray
      val id = new String(wkTitle)
      (id, outLinks)
    }).cache


    val vertices = wiki.map(tup => {
      val title = tup._1
      (pageToID(title), title)
    }).cache


    val edges: RDD[Edge[Double]] = wiki.flatMap(tup => {
      val pageId = pageToID(tup._1)
      val info = tup._2.iterator
      info.map(x => Edge(pageId, pageToID(x), 1.0))
    }).cache


    val graph = Graph(vertices, edges, "").subgraph(vpred = { (v, d) => d.nonEmpty }).cache
    val prGraph = graph.staticPageRank(iterations).cache;
    val prTitle = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }


    val finalResult = prTitle.vertices.top(100) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }


    context.parallelize(finalResult).map(t => t._2._2 + ": " + t._2._1).saveAsTextFile(outputFile)
  }
}
