import org.apache.spark.rdd.RDD
import java.lang.Math

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
