import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext
import org.apache.spark.sql._ // needed to save as Parquet format

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
  def saveParquet( filename: String, ext: String,
    graph: Graph, part: Partition, sc: SparkContext ): Unit = {
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    graph.vertices.toDF.write.parquet( s"$filename-graph-vertices$ext" )
    graph.edges.toDF.write.parquet( s"$filename-graph-edges$ext" )
    part.vertices.saveAsTextFile( s"$filename-part-vertices$ext" )
    part.edges.saveAsTextFile( s"$filename-part-edges$ext" )
  }
  def saveRDD( filename: String, ext: String,
  graph: Graph, part: Partition ): Unit = {
    graph.vertices.saveAsTextFile( s"$filename-graph-vertices$ext" )
    graph.edges.saveAsTextFile( s"$filename-graph-edges$ext" )
    part.vertices.saveAsTextFile( s"$filename-part-vertices$ext" )
    part.edges.saveAsTextFile( s"$filename-part-edges$ext" )
  }

  // save as local text file
  // only vertex data.are saved,
  // since edge data can be saved via Json graphs
  def saveTxt( filename: String, ext: String, graph: Graph ): Unit = {
    // for spacing consistency, needs to pad vertex names with spacing
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

    txtFile.write(s"Index   | ${pad("Name",maxNameLength)} | Module \n")
    txtFile.write(s"Index   | Module \n")

    for( vertex <- vertices ) {
      vertex match {
        // case (idx,(name,module)) => txtFile.write(
        //   "%7d | %s | %7d\n".format( idx, pad(name,maxNameLength), module )
        // )
        case (idx,(name,module)) => txtFile.write(
          "%d %d\n".format(idx, module)
        )
        case (idx,(name,module)) => txtFile.write("%d %d\n".format( idx, module ))
      }
    }
    txtFile.close
  }

  def saveFullJson( filename: String, ext: String, graph: Graph ) = {
    // fake nodes to preserve group ordering/coloring
    val fakeNodes = graph.vertices.map {
      case (idx,_) => (-idx,("",idx,0L,0.0))
    }
    .collect
    val vertices = graph.vertices.map {
      case (id,(name,module)) => (id,(name,module,1L,1.0))
    }
    .collect ++fakeNodes
    val edges = graph.edges.map {
      case (from,(to,weight)) => ((from,to),weight)
    }
    .collect.sorted
    val newGraph = JsonGraph( vertices.sorted, edges )
    JsonGraphWriter( s"$filename$ext", newGraph )
  }

  /***************************************************************************
   * save graph as Json for visualization
   * each node is a module
   * names are always empty string
   ***************************************************************************/
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
