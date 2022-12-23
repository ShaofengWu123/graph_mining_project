import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.util.parsing.json._
import scala.io.Source
import scala.collection.mutable.ListBuffer

import java.io.FileNotFoundException
import java.io.File
import java.lang.Math

// parent class of infomap, contains some helper functions for infomap
abstract class InfoMap_Utils 
{
  def apply( graph: Graph, part: Partition, logFile: LogFile )
  : ( Graph, Partition )
}

object InfoMap_Utils {

  // math functions
  def log( double: Double ) = Math.log(double)/Math.log(2.0)
  def plogp( double: Double ) = double*log(double)

  def codelen_calculate(
    vertices: RDD[(Long,(Long,Double,Double,Double))], probSum: Double
  ): Double = {

    if( vertices.count == 1 ) {
      // the entire graph is merged into one module
      -probSum
    }
    else {
      val qsum = vertices.map {
        case (_,(_,_,_,q)) => q
      }
      .sum
      val sum_others = vertices.map {
        case (_,(_,p,_,q)) => -2*plogp(q) +plogp(p+q)
      }
      .sum
      sum_others +plogp(qsum) -probSum
    }
  }

  def Q_calculate( node_num: Long, n: Long, p: Double, tele: Double, w: Double ) = tele*(node_num-n)/(node_num-1)*p +(1-tele)*w

  def detltaL_calculate(part: Partition, n1: Long, n2: Long, p1: Double, p2: Double, w12: Double, qsum: Double, q1: Double, q2: Double) = {
    val q12 = Q_calculate(part.node_num, n1+n2, p1+p2, part.tele, w12 )

    if( q12 > 0 && qsum+q12-q1-q2>0 ) (
      +plogp( qsum +q12-q1-q2 ) -plogp(qsum)
      -2*plogp(q12) +2*plogp(q1) +2*plogp(q2)
      +plogp(p1+p2+q12) -plogp(p1+q1) -plogp(p2+q2)
    )
    else 
    {
      -part.probSum -part.codelength
    }
  }
}

// infomap algorithm
class InfoMap extends InfoMap_Utils with Serializable
{
  //def this( config: JsonObj ) = this
  def this( config: JsonObj ) = this

  case class Merge
  (
    n1: Long, n2: Long, p1: Double, p2: Double,
    w1: Double, w2: Double, w1221: Double,
    q1: Double, q2: Double, dL: Double
  )

  def apply( graph: Graph, part: Partition, logFile: LogFile )
  : ( Graph, Partition ) = {
    //logFile.write(s"Using InfoMap algorithm\n",false)
    @scala.annotation.tailrec
    def merge_recur(loop: Int,qsum: Double,graph: Graph,part: Partition,mergeList: RDD[((Long,Long),Merge)]): ( Graph, Partition ) = {

      trim( loop, graph, part, mergeList )

      //logFile.write(s"State $loop: code length ${part.codelength}\n",false)
      //logFile.save( graph, part, true, loop.toString )

      if( mergeList.count == 0 )
        return terminate( loop, logFile, graph, part )

      val merge = merge_find(mergeList)
      if( merge._2.dL > 0 )
        return terminate( loop, logFile, graph, part )

      // logFile.write(
      //   s"Merge $loop: merging modules ${merge._1._1} and ${merge._1._2}"
      //   +s" with code length reduction ${merge._2.dL}\n",
      // false )

      val new_qsum = qsum_calculate( part, merge, qsum)
      val new_part = part_calculate( part, merge)
      val new_graph = graph_calculate( graph, merge)
      val new_mergel = mergelist_update(merge, mergeList, new_part, new_qsum)
      merge_recur( loop+1, new_qsum, new_graph, new_part, new_mergel )
    }

    def mergel_gen( part: Partition, qsum: Double ) = {
      // merges are  nondirectional edges
      part.edges.map {
        case (from,(to,weight)) =>
          if( from < to ) ((from,to),weight)
          else ((to,from),weight)
      }
      // via aggregation
      .reduceByKey(_+_)
      // now get associated vertex properties
      .map {
        case ((m1,m2),w1221) => (m1,(m2,w1221))
      }
      .join( part.vertices ).map {
        case (m1,((m2,w1221),(n1,p1,w1,q1)))
        => (m2,(m1,n1,p1,w1,q1,w1221))
      }
      .join( part.vertices ).map {
        case (m2,((m1,n1,p1,w1,q1,w1221),(n2,p2,w2,q2))) =>
        ((m1,m2),
        Merge(
          n1,n2,p1,p2,w1,w2,w1221,q1,q2,
          // calculate dL
          InfoMap_Utils.detltaL_calculate(
            part, n1,n2,p1,p2, w1+w2-w1221, qsum,q1,q2 ))
        )
      }
    }

    def trim( loop: Int, graph: Graph,
      part: Partition, mergeList: RDD[((Long,Long),Merge)] ): Unit = {
      if( loop%10 == 0 ) {
        graph.vertices.localCheckpoint
        val force1 = graph.vertices.count
        graph.edges.localCheckpoint
        val force2 = graph.edges.count
        part.vertices.localCheckpoint
        val force3 = part.vertices.count
        part.edges.localCheckpoint
        val force4 = part.edges.count
        mergeList.localCheckpoint
        val force5 = mergeList.count
     }
     {}
    }

    def terminate( loop: Int, logFile: LogFile,
      graph: Graph, part: Partition ) = {
      //logFile.write( s"Merging terminates after ${loop} merges\n", false )
      ( graph, part )
    }

    def graph_calculate(graph: Graph, merge: ((Long,Long),Merge) ) = {
      Graph(
        graph.vertices.map {
          case (idx,(name,module)) =>
            if( module==merge._1._1 || module==merge._1._2 )
              (idx,(name,merge._1._1))
            else
              (idx,(name,module))
        },
        graph.edges
      )
    }

    def merge_find( mergeList: RDD[((Long,Long),Merge)] ) = {
      mergeList.reduce {
        case (
          ((merge1A,merge2A),
            Merge(n1A,n2A,p1A,p2A,w1A,w2A,w1221A,q1A,q2A,dLA)),
          ((merge1B,merge2B),
            Merge(n1B,n2B,p1B,p2B,w1B,w2B,w1221B,q1B,q2B,dLB))
        )
        => {
          if( dLA < dLB )
            ((merge1A,merge2A),
              Merge(n1A,n2A,p1A,p2A,w1A,w2A,w1221A,q1A,q2A,dLA))
          else if( dLA > dLB )
            ((merge1B,merge2B),
              Merge(n1B,n2B,p1B,p2B,w1B,w2B,w1221B,q1B,q2B,dLB))
          else if( merge1A < merge1B )
            ((merge1A,merge2A),
              Merge(n1A,n2A,p1A,p2A,w1A,w2A,w1221A,q1A,q2A,dLA))
          else
            ((merge1B,merge2B),
              Merge(n1B,n2B,p1B,p2B,w1B,w2B,w1221B,q1B,q2B,dLB))
        }
      }
    }


    def mergelist_update(
      merge: ((Long,Long),Merge),
      mergeList: RDD[((Long,Long),Merge)],
      part: Partition,
      qsum: Double
    ) = {
      // grab new modular properties
      val merge1 = merge._1._1
      val merge2 = merge._1._2
      val N12 = merge._2.n1 +merge._2.n2
      val P12 = merge._2.p1 +merge._2.p2
      val W12 = merge._2.w1 +merge._2.w2 -merge._2.w1221
      val Q12 = InfoMap_Utils.Q_calculate(
        part.node_num, N12, P12, part.tele, W12 )

      mergeList.filter {
        // delete the merged edge, ie, (merge1,merge2)
        case ((m1,m2),_) => !( m1==merge1 && m2==merge2 )
      }
      .map {
        // entries associated to merge2 now is associated to merge1
        // and put in newly merged quantities to replace old quantities
        // anyway dL always needs to be recalculated
        case ((m1,m2),Merge(n1,n2,p1,p2,w1,w2,w1221,q1,q2,_)) =>
          if( m1==merge1 || m1==merge2 )
            ((merge1,m2),Merge(N12,n2,P12,p2,W12,w2,w1221,Q12,q2,0.0))
          else if( m2==merge1 )
            ((m1,merge1),Merge(n1,N12,p1,P12,w1,W12,w1221,q1,Q12,0.0))
          else if( m2==merge2 ) {
            if( merge1 < m1 )
              ((merge1,m1),Merge(N12,n1,P12,p1,W12,w1,w1221,Q12,q1,0.0))
            else
              ((m1,merge1),Merge(n1,N12,p1,P12,w1,W12,w1221,q1,Q12,0.0))
          }
          else
            ((m1,m2),Merge(n1,n2,p1,p2,w1,w2,w1221,q1,q2,0.0))
      }
      // aggregate inter-modular connection weights
      .reduceByKey {
        case (
          Merge(n1,n2,p1,p2,w1,w2,w12,q1,q2,_),
          Merge(_,_,_,_,_,_,w21,_,_,_)
        )
        => Merge(n1,n2,p1,p2,w1,w2,w12+w21,q1,q2,0.0)
      }
      // calculate dL
      .map {
        case (
          (m1,m2),
          Merge(n1,n2,p1,p2,w1,w2,w1221,q1,q2,_)
        ) => (
          (m1,m2),
          Merge(n1,n2,p1,p2,w1,w2,w1221,q1,q2,
              InfoMap_Utils.detltaL_calculate(part,
                n1,n2,p1,p2,w1+w2-w1221,qsum,q1,q2))
        )
      }
    }

    def part_calculate( part: Partition, merge: ((Long,Long),Merge) ) = {
      val newVertices = {
        // calculate properties of merged module
        val n12 = merge._2.n1 +merge._2.n2
        val p12 = merge._2.p1 +merge._2.p2
        val w12 = merge._2.w1 +merge._2.w2 -merge._2.w1221
        val q12 = InfoMap_Utils.Q_calculate(
          part.node_num, n12, p12, part.tele, w12 )

        // delete merged module
        part.vertices.filter {
          case (idx,_) => idx != merge._1._2
        }
        // put in new modular properties for merged module
        .map {
          case (idx,(n,p,w,q)) =>
            if( idx == merge._1._1 )
              (idx,(n12,p12,w12,q12))
            else
              (idx,(n,p,w,q))
        }
      }

      val newEdges = {
        val m1 = merge._1._1
        val m2 = merge._1._2

        part.edges
        // delete merged edges
        .filter {
          case (from,(to,_)) =>
            !( ( from==m1 && to==m2 ) || ( from==m2 && to==m1 ) )
        }
        .map {
          case (from,(to,weight)) => 
            val newFrom = if( from==m2 ) m1 else from
            val newTo = if( to==m2 ) m1 else to
            ((newFrom,newTo),weight)
        }
        // aggregate
        .reduceByKey(_+_)
        .map {
          case ((from,to),weight) => (from,(to,weight))
        }
      }

      Partition(
        part.node_num, part.tele,
        newVertices, newEdges,
        part.probSum,
        part.codelength +merge._2.dL
      )
    }

    def qsum_calculate(
      part: Partition, merge: ((Long,Long),Merge), qsum: Double )
    = {
      val n12 = merge._2.n1 +merge._2.n2
      val p12 = merge._2.p1 +merge._2.p2
      val w12 = merge._2.w1 +merge._2.w2 -merge._2.w1221
      val q12 = InfoMap_Utils.Q_calculate(
        part.node_num, n12, p12, part.tele, w12 )
      val q1 = merge._2.q1
      val q2 = merge._2.q2
      qsum +q12 -q1 -q2
    }

    val qsum = part.vertices.map {
      case (_,(_,_,_,q)) => q
    }.sum
    val edgeList = mergel_gen( part, qsum )

    merge_recur( 0, qsum, graph, part, edgeList )
  }
}


sealed case class Partition
(
  node_num: Long, tele: Double,
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

    val node_num: Long = graph.vertices.count

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
    val vertices: RDD[(Long,(Long,Double,Double,Double))] = {

      val exitw: RDD[(Long,Double)] = edges
      .join( ergodicFreq )
      .map {
        case (from,((to,weight),ergodicFreq)) => (from,ergodicFreq*weight)
      }
      .reduceByKey(_+_)

      ergodicFreq.leftOuterJoin(exitw)
      .map {
        //case (idx,(freq,Some(w))) => (idx,(1,freq,w,tele*freq+(1-tele)*w))
        case (idx,(freq,Some(_))) => (idx,(1,freq,freq,freq))
        case (idx,(freq,None))
        => if( node_num > 1) (idx,(1,freq,0,tele*freq))
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

    val codelength = InfoMap_Utils.codelen_calculate( vertices, probSum )

    // return Partition object
    Partition(
      node_num, tele,
      vertices, exitw,
      probSum, codelength
    )
  }
}



sealed case class Graph
(
  vertices: RDD[(Long,(String,Long))], // | index , name , module |
  edges: RDD[(Long,(Long,Double))] // | index from , index to , weight |
)

// graph reader definitions
object GraphReader
{
  def apply( sc: SparkContext, filename: String, logFile: LogFile ): Graph = {
    val regex = """(.*)\.(\w+)""".r
    val graph: Graph = filename match {
      case regex(_,ext) => {
        if( ext.toLowerCase == "net" )
          PajekReader( sc, filename, logFile )
        else
          throw new Exception(
            "File must be Pajek net file (.net)"
          )
      }
      case _ => throw new Exception("Graph file has no file extension")
    }
    graph.vertices.localCheckpoint
	graph.vertices.cache
    val force1 = graph.vertices.count
    graph.edges.localCheckpoint
	graph.edges.cache
    val force2 = graph.edges.count
    graph
  }
}


object PageRank
{
  def apply(
    graph: Graph, damping: Double, errThFactor: Double, logFile: LogFile
  ): RDD[(Long,Double)] = {
	logFile.write(s"Calculating PageRank\n",false)
    logFile.write(s"PageRank teleportation probablity ${1-damping}\n",false)
    logFile.write(s"PageRank error threshold factor $errThFactor\n",false)

    val nodeNumber: Long = graph.vertices.count
    val edges: Matrix = {
      val outLinkTotalWeight = graph.edges.map {
        case (from,(to,weight)) => (from,weight)
      }
      .reduceByKey(_+_)
      outLinkTotalWeight.cache

      // nodes without outbound links are dangling"
      val dangling: RDD[Long] = graph.vertices
      .leftOuterJoin(outLinkTotalWeight)
      .filter {
        case (_,(_,Some(_))) => false
        case (_,(_,None)) => true
      }
      .map {
        case (idx,_) => idx
      }

      // dangling nodes jump to uniform probability
      val constCol = dangling.map (
        x => ( x, 1.0/nodeNumber.toDouble )
      )

      // normalize the edge weights
      val normMat = graph.edges.join(outLinkTotalWeight)
      .map {
        case (from,((to,weight),totalweight)) => (from,(to,weight/totalweight))
      }

      outLinkTotalWeight.unpersist()

      Matrix( normMat, constCol )
    }
	edges.sparse.cache
	edges.constCol.cache

    // start with uniform ergodic frequency
    val freqUniform = graph.vertices.map {
      case (idx,_) => ( idx, 1.0/nodeNumber.toDouble )
    }
	freqUniform.cache

    // calls inner PageRank calculation function
    PageRank( edges, freqUniform, nodeNumber, damping,
      1.0/nodeNumber.toDouble/errThFactor,
      logFile, 0
    )
  }

  @scala.annotation.tailrec
  def apply(
    edges: Matrix, freq: RDD[(Long,Double)],
    n: Long, damping: Double, errTh: Double,
	logFile: LogFile, loop: Long
  ): RDD[(Long,Double)] = {
    // print the PageRank iteration number only in debug log
	logFile.write(s"Calculating PageRank, iteration $loop\n",false)

    // 2D Euclidean distance between two vectors
    def dist2D( v1: RDD[(Long,Double)], v2: RDD[(Long,Double)] ): Double = {
      val diffSq = (v1 join v2).map {
        case (idx,(e1,e2)) => (e1-e2)*(e1-e2)
      }
      .sum
      Math.sqrt(diffSq)
    }

    // create local checkpoint to truncate RDD lineage
    freq.localCheckpoint
	freq.cache
    val forceEval = freq.count

    // the random walk contribution of the ergodic frequency
    val stoFreq = edges *freq
    // the random jump contribution of the ergodic frequency
    val bgFreq = freq.map {
      case (idx,_) => (idx, (1.0-damping)/n.toDouble )
    }

    // combine both random walk and random jump contributions
    val newFreq = (bgFreq leftOuterJoin stoFreq).map {
      case (idx,(bg,Some(sto))) => ( idx, bg+ sto*damping )
      case (idx,(bg,None)) => ( idx, bg )
    }
	newFreq.cache

    // recursive call until freq converges wihtin error threshold
    val err = dist2D(freq,newFreq)

    if( err < errTh ) newFreq
    else PageRank( edges, newFreq, n, damping, errTh, logFile, loop+1 )
  }
}



sealed case class Matrix
( sparse: RDD[(Long,(Long,Double))],
  constCol: RDD[(Long,Double)] )
extends Serializable {
  def *( vector: RDD[(Long,Double)] ): RDD[(Long,Double)] = {

    // constCol is an optimization,
    // if all entries within a column has constant value
    val constColProd = (constCol join vector).map {
      case (from,(col,vec)) => col*vec
    }
    .sum

    val constColVec = vector.map {
      case (idx,x) => (idx,constColProd)
    }

    val matTimesVec = (sparse join vector).map {
      case (from,((to,matrix),vec)) => (to,vec*matrix)
    }
    .reduceByKey(_+_)

    val matTimesVecPlusConstCol = (matTimesVec rightOuterJoin constColVec)
    .map {
      case (idx,(Some(x),col)) => (idx,x+col)
      case (idx,(None,col)) => (idx,col)
    }

    matTimesVecPlusConstCol
  }
}


/*****************************************************************************
 * Pajek net file reader
 * file is assumed to be local and read in serially
 *****************************************************************************/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.io.Source
import java.io.FileNotFoundException

import scala.collection.mutable.ListBuffer

import java.io.File

object PajekReader
{
  def apply( sc: SparkContext, filename: String, logFile: LogFile ): Graph = {
    try {

      var vertices = new ListBuffer[(Long,(String,Long))]()
      var edges = new ListBuffer[((Long,Long),Double)]()

      // regexes to match lines in file
      val starRegex = """\*([a-zA-Z]+).*""".r
      val verticesRegex = """(?i)\*Vertices[ \t]+([0-9]+)""".r
      val vertexRegex = """[ \t]*?([0-9]+)[ \t]+\"(.*)\".*""".r
      val edgeRegex1 = """[ \t]*?([0-9]+)[ \t]+([0-9]+)[ \t]*""".r
      val edgeRegex2 = """[ \t]*?([0-9]+)[ \t]+([0-9]+)[ \t]+([0-9.eE\-\+]+).*""".r

      var section: String = "__begin"

      var nodeNumber: Long = -1

      var lineNumber = 1 // line number in file, used when printing file error
      // read file serially
      for( line <- Source.fromFile(new File(filename), "ISO-8859-1").getLines
        if( line != null && !line.isEmpty // skip empty line
          && line.charAt(0) != '%' // skip comments
        )
      ) {

        val newSection = line match {
          // line is section declarator, modify section
          case starRegex(id) => {
            line match {
              case starRegex(expr) => {
                val newSection = expr.toLowerCase
                // check that new section is valid
                if( newSection!="vertices"
                  && newSection!="arcs" && newSection!="arcslist"
                  && newSection!="edges" && newSection!="edgeslist"
                )
                  throw new Exception( "Pajek file format only accepts"
                    +" Vertices, Arcs, Edges, Arcslist, Edgeslist"
                    +" as section declarator: line "+lineNumber )
                // check there is no more than one vertices section
                if( newSection == "vertices" ) {
                  if( nodeNumber != -1 )
                    throw new Exception(
                      "There must be one and only one vertices section"
                    )
                  // read nodeNumber
                  nodeNumber = line match {
                    case verticesRegex(expr) => expr.toLong
                    case _ => throw new Exception(
                      s"Cannot read node number: line $lineNumber"
                    )
                  }
                }
                section = "section_def"
                newSection
              }
            }
          }
          // line is not section declarator,
          // section does not change
          case _ => section
        }

        if( section == "vertices" ) {
          val newVertex = line match {
            case vertexRegex( idx, name ) =>
              if( 1<=idx.toLong && idx.toLong<=nodeNumber )
                ( idx.toLong, (name,idx.toLong) )
              // check that index is in valid range
              else throw new Exception(
                s"Vertex index must be within [1,$nodeNumber]: line $lineNumber"
              )
            // check vertex parsing is correct
            case _ => throw new Exception(
              s"Vertex definition error: line $lineNumber"
            )
          }
          vertices += newVertex
        }

  /***************************************************************************
   * Read edge information
   ***************************************************************************/
        else if( section=="edges" || section=="arcs" ) {
          val newEdge = line match {
            case edgeRegex1( src, dst ) =>
              // check that index is in valid range
              if( 1<=src.toLong && src.toLong<=nodeNumber
               && 1<=dst.toLong && dst.toLong<=nodeNumber )
                ( ( src.toLong, dst.toLong ), 1.0 )
              else throw new Exception(
                s"Vertex index must be within [1,$nodeNumber]: line $lineNumber"
              )
            case edgeRegex2( src, dst, weight ) =>
              // check that index is in valid range
              if( 1<=src.toLong && src.toLong<=nodeNumber
               && 1<=dst.toLong && dst.toLong<=nodeNumber ) {
                // check that weight is not negative
                if( weight.toDouble < 0 ) throw new Exception(
                  s"Edge weight must be non-negative: line $lineNumber"
                )
                ( ( src.toLong, dst.toLong ), weight.toDouble )
              }
              else throw new Exception(
                s"Vertex index must be within [1,$nodeNumber]: line $lineNumber"
              )
            // check vertex parsing is correct
            case _ => throw new Exception(
              s"Edge definition error: line $lineNumber"
            )
          }
          edges += newEdge
        }

  /***************************************************************************
   * Read edge list information
   ***************************************************************************/
        else if( section=="edgeslist" || section=="arcslist" ) {
          // obtain a list of vertices
          val vertices = line.split("\\s+").filter(x => !x.isEmpty)
          // obtain a list of edges
          val newEdges = vertices.slice( 1, vertices.length )
          .map {
            case toVertex => ( ( vertices(0).toLong, toVertex.toLong ), 1.0 )
          }
          // append new list to existing list of edges
          edges ++= newEdges
        }

        else if( section != "section_def" )
        {
          throw new Exception(
            s"Line $lineNumber does not belong to any sections"
          )
        }

  /***************************************************************************
   * prepare for next loop
   ***************************************************************************/
        section = newSection
        lineNumber += 1
      }

  /***************************************************************************
   * check there is at least one vertices section
   ***************************************************************************/
      if( nodeNumber == -1 )
        throw new Exception("There must be one and only one vertices section")

      logFile.write("Finished reading from disk; parallelizing...\n",false)

      val verticesRDD: RDD[(Long,(String,Long))] = {
        // initiate array
        val verticesArray = new Array[(Long,(String,Long))](nodeNumber.toInt)
        for( idx <- 1 to nodeNumber.toInt )
          verticesArray( idx-1 ) = (-1L,("",-1L))
        // put in each vertices list element to array
        // and check for duplication
        for( (idx,(name,module)) <- vertices ) {
          if( verticesArray(idx.toInt-1)._1 != -1 )
            throw new Exception(
              s"Vertex ${verticesArray(idx.toInt-1)._1} is not unique!"
            )
          verticesArray( idx.toInt-1 ) = ( idx, (name,module) )
        }

        for( idx <- 1 to nodeNumber.toInt )
          if( verticesArray( idx-1 )._1 == -1 )
            verticesArray( idx-1 ) = ( idx, (idx.toString,idx) )
        // convert to RDD
        sc.parallelize( verticesArray )
      }
	  verticesRDD.cache


      val edgesRDD: RDD[(Long,(Long,Double))] = sc.parallelize(edges)
      .reduceByKey(_+_)
      .map {
        case ((from,to),weight) => (from,(to,weight))
      }
	  edgesRDD.cache


      Graph( verticesRDD, edgesRDD )
    }
    catch {
        case e: FileNotFoundException =>
          throw new Exception("Cannot open file "+filename)
    }
  }
}
