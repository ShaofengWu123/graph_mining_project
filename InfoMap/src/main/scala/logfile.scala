import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext
//import org.apache.spark.sql._ // needed to save as Parquet format

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
    graph: Graph,
    // part: reduced graph, where each node is a community
    part: Partition,
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
      if( !pathReducedJson.isEmpty ) {
        val (filename,ext) = splitFilepath(pathReducedJson)
        LogFile.saveReducedJson( filename, exext+ext, part )
      }
    }
  }
}

object LogFile
{
  def saveTxt( filename: String, ext: String, graph: Graph ): Unit = {
    def pad( string: String, totalLength: Int ): String = {
      var padding = ""
      for( i <- string.length+1 to totalLength )
        padding += " "
      string +padding
    }
    val file = new File( s"$filename$ext" )
    val txtFile = new PrintWriter(file)

    val vertices = graph.vertices.collect.sorted
    val maxNameLength = Math.max( graph.vertices.reduce {
      case ( (idx1,(name1,mod1)), (idx2,(name2,mod2)) ) =>
        if( name1.length > name2.length )
          (idx1,(name1,mod1))
        else
          (idx2,(name2,mod2))
    }
    ._2._1.length, 4 )

    //txtFile.write(s"Index   | Module \n")

    for( vertex <- vertices ) {
      vertex match {
        // case (idx,(name,module)) => txtFile.write(
        //   "%7d | %s | %7d\n".format( idx, pad(name,maxNameLength), module )
        // )
        case (idx,(name,module)) => txtFile.write("%d %d\n".format( idx, module ))
      }
    }
    txtFile.close
  }

  def saveReducedJson( filename: String, ext: String, part: Partition ) = {
    val vertices = part.vertices.map {
      case (id,(n,p,_,_)) => (id,(id.toString,id,n,p))
    }
    // Json is local file
    .collect.sorted
    val edges = part.edges.map {
      case (from,(to,weight)) => ((from,to),weight)
    }
    .collect.sorted
    val graph = JsonGraph( vertices, edges )
    JsonGraphWriter( s"$filename$ext", graph )
  }
}
