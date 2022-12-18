import scala.util.parsing.json._

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
