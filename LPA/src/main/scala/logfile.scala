import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.SparkContext

import java.util.Calendar

import java.io._

sealed class LogFile(
  val sc:               SparkContext, 
  val pathLog:          String, 
  val pathTxt:          String, 
  val pathReducedJson:  String, 
  val debug:            Boolean 
)
{

  // create file to store the loop of code lengths
  val logFile = if( !pathLog.isEmpty ) {
    val file = new File( pathLog )
    new PrintWriter(file)
  }
  else null

  def write( msg: String, debugging: Boolean )
    = if( !pathLog.isEmpty && ( !debugging || debug ) ) {
      logFile.append(s"${Calendar.getInstance().getTime}: "+msg)
      logFile.flush
    }
  def close = if( !pathLog.isEmpty ) logFile.close

  def save(
    graph: Graph[VertexId, ED],
    // part: reduced graph, where each node is a community
    debugging: Boolean,
    debugExt: String // this string is appended to file name (for debugging)
  ): Unit = {

    def splitFilepath( filepath: String ): (String,String) = {
      val regex = """(.*)\.(\w+)""".r
      filepath match {
        case regex(path,ext) => ( path, "."+ext )
        case _ => ( filepath, "" )
      }
    }

    if( !debugging || debug ) {
      val exext = if(debugging) debugExt else ""
      if( !pathTxt.isEmpty ) {
        val (filename,ext) = splitFilepath(pathTxt)
        LogFile.saveTxt( filename, exext+ext, graph )
      }
    }
  }
}

object LogFile
{
  def saveTxt( filename: String, ext: String, graph: Graph[VD, ED] ): Unit = {
    def pad( string: String, totalLength: Int ): String = {
      var padding = ""
      for( i <- string.length+1 to totalLength )
        padding += " "
      string +padding
    }
    val file = new File( s"$filename$ext" )
    val txtFile = new PrintWriter(file)

    graph.vertices.sortBy(_._2).foreach {
        case (id, (group)) => txtFile.write("%d %d\n".format(id, group ))
    }
    txtFile.close
  }
}
