import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

sealed case class Partition
(
  nodeNumber: Long, tele: Double,
  // | idx , n , p , w , q |
  vertices: RDD[(Long,(Long,Double,Double,Double))],
  // | index from , index to , weight |
  edges: RDD[(Long,(Long,Double))],
  // sum of plogp(ergodic frequency), for codelength calculation
  probSum: Double,
  codelength: Double // codelength given the modular partitioning
)

object Partition
{
  def init( graph: Graph, pageRankConfig: JsonObj, logFile: LogFile )
  : Partition = init(
    graph, 
    pageRankConfig.getObj("tele").value.toString.toDouble,
    pageRankConfig.getObj("error threshold factor").value.toString.toDouble,
    logFile
  )
  def init(
	graph: Graph, tele: Double, errThFactor: Double,
	logFile: LogFile
  ): Partition = {

    val nodeNumber: Long = graph.vertices.count

    // filter away self-connections
    // and normalize edge weights per "from" node
    val edges = {
      val nonselfEdges = graph.edges.filter {
        case (from,(to,weight)) => from != to
      }
      val outLinkTotalWeight = nonselfEdges.map {
        case (from,(to,weight)) => (from,weight)
      }
      .reduceByKey(_+_)
      nonselfEdges.join(outLinkTotalWeight).map {
        case (from,((to,weight),norm)) => (from,(to,weight/norm))
      }
    }
	edges.cache

    // exit probability from each vertex
    val ergodicFreq = PageRank(
      Graph(graph.vertices,edges),
      1-tele, errThFactor, logFile
	)
    ergodicFreq.cache

    // modular information
    // since transition probability is normalized per "from" node,
    // w and q are mathematically identical to p
    // as long as there is at least one connection
    // | id , size , prob , exitw , exitq |
    val vertices: RDD[(Long,(Long,Double,Double,Double))] = {

      val exitw: RDD[(Long,Double)] = edges
      .join( ergodicFreq )
      .map {
        case (from,((to,weight),ergodicFreq)) => (from,ergodicFreq*weight)
      }
      .reduceByKey(_+_)

      // since exitw is normalized per "from" node,
      // exitw is always (from,freq)
      // so calculations can be simplified
      ergodicFreq.leftOuterJoin(exitw)
      .map {
        //case (idx,(freq,Some(w))) => (idx,(1,freq,w,tele*freq+(1-tele)*w))
        case (idx,(freq,Some(_))) => (idx,(1,freq,freq,freq))
        case (idx,(freq,None))
        => if( nodeNumber > 1) (idx,(1,freq,0,tele*freq))
           else (idx,(1,1,0,0))
      }
    }
	val forceEval = vertices.count
	vertices.localCheckpoint
	vertices.cache

    val exitw = edges.join(ergodicFreq).map {
      case (from,((to,weight),freq)) => (from,(to,freq*weight))
    }
	exitw.cache

    val probSum = ergodicFreq.map {
      case (_,p) => InfoMap_Utils.plogp(p)
    }
    .sum

    ergodicFreq.unpersist()

    val codelength = InfoMap_Utils.calCodelength( vertices, probSum )

    // return Partition object
    Partition(
      nodeNumber, tele,
      vertices, exitw,
      probSum, codelength
    )
  }
}
