import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.xml.{ XML, NodeSeq }

object PageRankSpark {
  def main(args: Array[String]) {

    val inputFile = args(0)
    val outputFile = args(1)
    val iterations = if (args.length > 2) args(2).toInt else 10
    val sparkConfig = new SparkConf()

    sparkConfig.setAppName("SparkPageRank")

    sparkConfig.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val context = new SparkContext(sparkConfig)

    val input = context.textFile(inputFile)


    var wiki = input.map(line => {
      val cols = line.split("\t")
      val (wkTitle, ngData) = (cols(0), cols(3).replace("\\n", "\n"))
      val links =
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

    var pageRanks = wiki.mapValues(v => 1.0)

    for (i <- 1 to iterations) {
      val contribs = wiki.join(pageRanks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
          pageRanks = contribs.reduceByKey(_+_).mapValues (0.15 + 0.85 * _)

      val output = pageRanks.takeOrdered(100)(Ordering[Double].reverse.on(x => x._2))
      context.parallelize(output).saveAsTextFile(outputFile)
      pageRanks.collect().foreach(tup => println(tup._1 + "    " + tup._2 + "."))
      context.stop()
    }
  }
}
