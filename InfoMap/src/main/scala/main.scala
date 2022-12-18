import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object InfoMapMain {
  def main(): Unit = {

    // use default or alternative config file name
    val configFileName = "config.json"
    val config = new JsonReader(configFileName)

  /***************************************************************************
   * Initialize structures; function definitions defined below
   ***************************************************************************/
    val graphFile = config.getObj("Graph").value.toString
    val( spark, context ) = init_spark( config.getObj("spark configs") )
  	val pageRankConfig = config.getObj("PageRank")
    val cdConfig = config.getObj("Community Detection")
    val logFile = initLog( context, config.getObj("log") )

  /***************************************************************************
   * read, solve, save
   ***************************************************************************/
    logEnvironment( spark, context, logFile )
    val graph0: Graph = readGraph( context, graphFile, logFile )
    val part0: Partition = initPartition( graph0, pageRankConfig, logFile )
    //val(graph1,part1) = communityDetection( graph0, part0, cdConfig, logFile )
    val algo = {
        new InfoMap( cdConfig )
    }
    val t1 = System.nanoTime 
    val(graph1,part1) = algo( graph0, part0, logFile )
    val duration = (System.nanoTime - t1) / 1e9d
    println(s"Running time: $duration s")   
    saveFinalGraph( graph1, part1, logFile )
    //terminate( context, logFile )
  }

/*****************************************************************************
 * Below functions are implementations of function calls above
 *****************************************************************************/

  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
    def init_spark( sparkConfig: JsonObj ): (SparkConf,SparkContext) = {
      val master = sparkConfig.getObj("Master").value.toString
      val numExecutors = sparkConfig.getObj("num executors").value.toString
      val executorCores = sparkConfig.getObj("executor cores").value.toString
      val driverMemory = sparkConfig.getObj("driver memory").value.toString
      val executorMemory = sparkConfig.getObj("executor memory").value.toString
      val spark = new SparkConf()
        //.setAppName("InfoFlow")
        .setAppName("InfoMap")
        .setMaster( master )
        .set( "spark.executor.instances", numExecutors )
        .set( "spark.executor.cores", executorCores )
        .set( "spark.driver.memory", driverMemory )
        .set( "spark.executor.memory", executorMemory )
      val sc = new SparkContext(spark)
      sc.setLogLevel("OFF")
      ( spark, sc )
    }

  /***************************************************************************
   * create log file object
   ***************************************************************************/
    def initLog( sc: SparkContext, logConfig: JsonObj ): LogFile = new LogFile(
      sc,
      logConfig.getObj("log path").value.toString,
      logConfig.getObj("Parquet path").value.toString,
      logConfig.getObj("RDD path").value.toString,
      logConfig.getObj("txt path").value.toString,
      logConfig.getObj("Full Json path").value.toString,
      logConfig.getObj("Reduced Json path").value.toString,
      logConfig.getObj("debug").value.toString.toBoolean
    )

  /***************************************************************************
   * log app version, spark version
   ***************************************************************************/
    def logEnvironment( spark: SparkConf, sc: SparkContext, logFile: LogFile )
    : Unit = {
      val jar = sc.jars.head.split('/').last
      val version = jar.split('-').last.split('.').dropRight(1).mkString(".")
      logFile.write(s"Running ${sc.appName}, version: $version\n",false)
      val jvmHeapSpace = Runtime.getRuntime().maxMemory/1024/1024
      logFile.write(
        s"Driver memory/Java heap size: $jvmHeapSpace Mb\n",
      false)
      logFile.write(s"Spark version: ${sc.version}\n",false)
      logFile.write(s"Spark configurations:\n",false)
      spark.getAll.foreach{ case (x,y) => logFile.write(s"$x: $y\n",false) }
    }

  /***************************************************************************
   * read in graph
   ***************************************************************************/
    def readGraph( sc: SparkContext, graphFile: String,
    logFile: LogFile ): Graph = {
      logFile.write(s"Reading $graphFile\n",false)
      val graph = GraphReader( sc, graphFile, logFile )
      val vertices = graph.vertices.count
      val edges = graph.edges.count
      logFile.write(
        s"Read in network with $vertices nodes and $edges edges\n",
      false)
      graph
    }

  /***************************************************************************
   * initialize partitioning
   ***************************************************************************/
    def initPartition( graph: Graph,
    pageRankConfig: JsonObj, logFile: LogFile ): Partition = {
      logFile.write(s"Initializing partitioning\n",false)
      val part = Partition.init( graph, pageRankConfig, logFile )
      logFile.write(s"Finished initialization calculations\n",false)
      part
    }

  /***************************************************************************
   * perform community detection
   ***************************************************************************/
    def communityDetection( graph: Graph, part: Partition,
    cdConfig: JsonObj, logFile: LogFile ): (Graph,Partition) = {
      val algoName = cdConfig.getObj("name").value.toString
      val algo = {
        if( algoName == "InfoMap" )
          new InfoMap( cdConfig )
        else if( algoName == "InfoFlow" )
          new InfoFlow( cdConfig )
        else throw new Exception(
          "Community detection algorithm must be InfoMap or InfoFlow"
        )
      }
      algo( graph, part, logFile )
    }

  /***************************************************************************
   * save final graph
   ***************************************************************************/
    def saveFinalGraph( graph: Graph, part: Partition, logFile: LogFile )
    : Unit = {
      logFile.write(s"Save final graph\n",false)
      logFile.write(s"with ${part.vertices.count} modules"
        +s" and ${part.edges.count} connections\n",
      false)
      logFile.save( graph, part, false, "" )
    }

  /***************************************************************************
   * terminate program
   ***************************************************************************/
    def terminate( sc: SparkContext, logFile: LogFile ): Unit = {
      logFile.write("InfoFlow Terminate\n",false)
      logFile.close
      sc.stop
    }
}