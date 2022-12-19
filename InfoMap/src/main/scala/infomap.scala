import org.apache.spark.rdd.RDD

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
    def recursiveMerge(
      loop: Int,
      qi_sum: Double,
      graph: Graph,
      part: Partition,
      mergeList: RDD[((Long,Long),Merge)]
    ): ( Graph, Partition ) = {

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

      val new_qi_sum = qsum_calculate( part, merge, qi_sum )
      val newPart = part_calculate( part, merge )
      val newGraph = graph_calculate( graph, merge )
      val newMergeList = mergelist_update(
        merge, mergeList, newPart, new_qi_sum )
      recursiveMerge( loop+1, new_qi_sum, newGraph, newPart, newMergeList )
    }

    def mergel_gen( part: Partition, qi_sum: Double ) = {
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
            part, n1,n2,p1,p2, w1+w2-w1221, qi_sum,q1,q2 ))
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
      qi_sum: Double
    ) = {
      // grab new modular properties
      val merge1 = merge._1._1
      val merge2 = merge._1._2
      val N12 = merge._2.n1 +merge._2.n2
      val P12 = merge._2.p1 +merge._2.p2
      val W12 = merge._2.w1 +merge._2.w2 -merge._2.w1221
      val Q12 = InfoMap_Utils.calQ(
        part.nodeNumber, N12, P12, part.tele, W12 )

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
                n1,n2,p1,p2,w1+w2-w1221,qi_sum,q1,q2))
        )
      }
    }

    def part_calculate( part: Partition, merge: ((Long,Long),Merge) ) = {
      val newVertices = {
        // calculate properties of merged module
        val n12 = merge._2.n1 +merge._2.n2
        val p12 = merge._2.p1 +merge._2.p2
        val w12 = merge._2.w1 +merge._2.w2 -merge._2.w1221
        val q12 = InfoMap_Utils.calQ(
          part.nodeNumber, n12, p12, part.tele, w12 )

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
        part.nodeNumber, part.tele,
        newVertices, newEdges,
        part.probSum,
        part.codelength +merge._2.dL
      )
    }

    def qsum_calculate(
      part: Partition, merge: ((Long,Long),Merge), qi_sum: Double )
    = {
      val n12 = merge._2.n1 +merge._2.n2
      val p12 = merge._2.p1 +merge._2.p2
      val w12 = merge._2.w1 +merge._2.w2 -merge._2.w1221
      val q12 = InfoMap_Utils.calQ(
        part.nodeNumber, n12, p12, part.tele, w12 )
      val q1 = merge._2.q1
      val q2 = merge._2.q2
      qi_sum +q12 -q1 -q2
    }

    val qi_sum = part.vertices.map {
      case (_,(_,_,_,q)) => q
    }.sum
    val edgeList = mergel_gen( part, qi_sum )

    recursiveMerge( 0, qi_sum, graph, part, edgeList )
  }
}
