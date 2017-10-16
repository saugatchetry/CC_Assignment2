import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.xml.{XML, NodeSeq}

object PageRankUniversities{
	def main(args: Array[String]){

		val inputFile = args(0)
		val outputFile = args(1)
		val universityDB = args(2)

		val iterations = if (args.length > 3) args(3).toInt else 10
		val sparkConf = new SparkConf().setAppName("PageRankUniversities")

		val context = new SparkContext(sparkConf)

		val input = context.textFile(inputFile)
		val universitiesList = context.textFile(univDB)



		val n = universitiesList.count()


    def pageToID(title: String): VertexId = {
      title.toLowerCase.replace(" ", "").hashCode.toLong
    }


		def extractData(line: String):(String, Array[String])={

			val cols = line.split("\t")

			val (univ_data, content) = (cols(1),cols(3).replace("\\n","\n"))

			val id = new String(univ_data)

			val links	=	if(content=="\\N"){
					NodeSeq.Empty
				}
				else{

						try{
								XML.loadString(content) \\ "link" \ "target"
							}catch{

						case e: org.xml.sax.SAXParseException=>
							System.err.println("Error")
						NodeSeq.Empty
					}
				}

			val outputLinks = links.map(link=>new String(link.text)).toArray

			(id, outputLinks)
		}
		val links = input.map(extractData _)


		val vertexTable:RDD[(VertexId, String)]=universitiesList.map(line=>{

			(pageToID(line),line)

		}).cache

		val edgeTable:RDD[Edge[Double]]=links.flatMap(line=>{

			val pageId=pageToID(line._1)

			line._2.iterator.map(x=>Edge(pageId, pageToID(x), 1.0/n))

		}).cache

		val myGraph = Graph(vertexTable, edgeTable, "").subgraph(vpred={(v,d)=>d.nonEmpty}).cache

		val prGraph = myGraph.staticPageRank(iterations).cache
		val title_PrGraph = myGraph.outerJoinVertices(prGraph.vertices){

			(v, title, rank)=>(rank.getOrElse(0.0), title)

		}

		val finalResult = title_PrGraph.vertices.top(100){

			Ordering.by((entry:(VertexId, (Double, String)))=>entry._2._1)

		}

		context.parallelize(result).map(t=>t._2._2+": "+t._2._1).saveAsTextFile(outputFile)
		finalResult.foreach(t => println(t._2._2+": "+t._2._1))
	}

}
