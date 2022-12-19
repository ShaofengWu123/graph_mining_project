import scala.util.parsing.json._
import java.io._

sealed case class JsonObj( value: Any ) {
  def getObj( keys: String* ): JsonObj = _getObj( keys.toList )
  def _getObj( keys: List[String] ): JsonObj = {
    try {
      keys match {
        case key::Nil =>
          JsonObj( value.asInstanceOf[Map[String,Any]] (key) )
        case key :: nextkeys => {
          val nextObj = JsonObj(
            value.asInstanceOf[Map[String,Any]] (key) )
          nextObj._getObj(nextkeys)
        }
    case Nil => throw new Exception("Json parsing error")
    }
  }
    catch {
      case e: java.lang.ClassCastException =>  throw e
      case e: Throwable => throw(e)
    }
  }
}

sealed class JsonReader( filename: String )
{
/*****************************************************************************
 * strategy is to read in whole file
 * then wrap it into a JsonObj, which can then be accessed
 *****************************************************************************/
  // read in whole file
  val wholeFile: String = {
    val source = scala.io.Source.fromFile(filename)
    try source.mkString
    finally source.close
  }

  // check Json file validity
  {
    val tryParse = JSON.parseFull(wholeFile)
    tryParse match {
      case Some(_) => {}
      case None => throw new Exception("Invalid Json file")
    }
  }

  private val jsonObj = JsonObj( JSON.parseFull(wholeFile).get )
  def getObj( keys: String* ): JsonObj = _getObj( keys.toList )
  def _getObj( keys: List[String] ): JsonObj = jsonObj._getObj(keys)
}

sealed case class JsonGraph
(
  // | id , name , module , couunt, size |
  vertices: Array[(Long,(String,Long,Long,Double))],
  // |from id , to id , weight |
  edges: Array[((Long,Long),Double)]
)

object JsonGraphWriter
{
  def apply( filename: String, graph: JsonGraph ): Unit = {

    // open file
    val file = new PrintWriter( new File(filename) )

    // write node data
    if( !graph.vertices.isEmpty ) {
      file.write( "{\n\t\"nodes\": [\n" )
      val nodeCount = graph.vertices.size
      for( idx <- 0 to nodeCount-1 ) {
        graph.vertices(idx) match {
          case (id,(name,module,count,size)) => {
            file.write(
              "\t\t{\"id\": \"" +s"$id" +"\", "
             +"\"size\": \"" +s"$size" +"\", "
             +"\"count\": \"" +s"$count" +"\", "
             +"\"name\": \"" +s"$name" +"\", "
             +"\"group\": \"" +s"$module" +"\""
             +"}"
            )
            if( idx < nodeCount-1 )
              file.write(",")
            file.write("\n")
          }
        }
      }
      file.write( "\t]" )
    }

    // write edge data
    if( !graph.edges.isEmpty ) {
      file.write( ",\n\t\"links\": [\n" )
      val edgeCount = graph.edges.size
      for( idx <- 0 to edgeCount-1 ) {
        graph.edges(idx) match {
          case ((from,to),weight) =>
            file.write(
              "\t\t{\"source\": \"" +s"$from" +"\", "
             +"\"target\": \"" +s"$to" +"\", "
             +"\"value\": \"" +s"$weight" +"\""
             +"}"
            )
            if( idx < edgeCount-1 )
              file.write(",")
            file.write("\n")
        }
      }
      file.write("\t]")
    }

    // close file
    file.write( "\n}" )
    file.close
  }
}

