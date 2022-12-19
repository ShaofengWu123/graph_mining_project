import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object InfoMapMain {
  def main(): Unit = {

    // use default or alternative config file name
    val config_file = "config.json"
    val config = new JsonReader(config_file)

    val graph_file = config.getObj("Graph").value.toString
    val( spark, context ) = init_spark( config.getObj("spark configs") )
  	val params_config = config.getObj("Params")
    val cdConfig = config.getObj("Community Detection")
    val logFile = init_log( context, config.getObj("log") )

    // debug
    //log_debug( spark, context, logFile )

    val initial_graph: Graph = graph_read( context, graph_file, logFile )
    val commu: Partition = subgraph_init( initial_graph, params_config, logFile )

    // create a new infomap instance 
    val algo = {
        new InfoMap( cdConfig )
    }
    // run infomap algorithm
    val t1 = System.nanoTime 
    val(result_graph,result_commu) = algo( initial_graph, commu, logFile )
    val duration = (System.nanoTime - t1) / 1e9d
    println(s"Running time: $duration s")   
    save_result( result_graph, result_commu, logFile)
  }

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

    def init_log( sc: SparkContext, logConfig: JsonObj ): LogFile = new LogFile(
      sc,
      logConfig.getObj("log path").value.toString,
      logConfig.getObj("Parquet path").value.toString,
      logConfig.getObj("RDD path").value.toString,
      logConfig.getObj("txt path").value.toString,
      logConfig.getObj("Full Json path").value.toString,
      logConfig.getObj("Reduced Json path").value.toString,
      logConfig.getObj("debug").value.toString.toBoolean
    )



    def graph_read( sc: SparkContext, graph_file: String,
    logFile: LogFile ): Graph = {
      logFile.write(s"Reading $graph_file\n",false)
      val graph = GraphReader( sc, graph_file, logFile )
      val vertices = graph.vertices.count
      val edges = graph.edges.count
      logFile.write(
        s"Read in network with $vertices nodes and $edges edges\n",
      false)
      graph
    }

    def subgraph_init( graph: Graph,
    params_config: JsonObj, logFile: LogFile ): Partition = {
      logFile.write(s"Initializing partitioning\n",false)
      val part = Partition.init( graph, params_config, logFile )
      logFile.write(s"Finished initialization calculations\n",false)
      part
    }


    def save_result( graph: Graph, part: Partition, logFile: LogFile )
    : Unit = {
      logFile.write(s"Save final graph\n",false)
      logFile.write(s"with ${part.vertices.count} modules"
        +s" and ${part.edges.count} connections\n",
      false)
      logFile.save( graph, part, false, "" )
    }
    
    // for debugging
    // def log_debug( spark: SparkConf, sc: SparkContext, logFile: LogFile )
    // : Unit = {
    //   val jar = sc.jars.head.split('/').last
    //   val version = jar.split('-').last.split('.').dropRight(1).mkString(".")
    //   logFile.write(s"Running ${sc.appName}, version: $version\n",false)
    //   val jvmHeapSpace = Runtime.getRuntime().maxMemory/1024/1024
    //   logFile.write(
    //     s"Driver memory/Java heap size: $jvmHeapSpace Mb\n",
    //   false)
    //   logFile.write(s"Spark version: ${sc.version}\n",false)
    //   logFile.write(s"Spark configurations:\n",false)
    //   spark.getAll.foreach{ case (x,y) => logFile.write(s"$x: $y\n",false) }
    // }

}