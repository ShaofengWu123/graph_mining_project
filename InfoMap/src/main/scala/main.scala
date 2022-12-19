import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object InfoMapMain {
  def main(args: Array[String]): Unit = {

    // use default or alternative config file name
    val config_file = "config.json"
    val config = new JsonReader(config_file)

    val graph_file = config.getObj("Graph").value.toString
    val( spark, context ) = init_spark( config.getObj("spark configs") )
  	val params_config = config.getObj("Params")
    val infomap_config = config.getObj("InfoMap")
    val log_file = init_log( context, config.getObj("log") )

    // debug
    //log_debug( spark, context, log_file )

    val initial_graph: Graph = graph_read( context, graph_file, log_file )
    val commu: Partition = subgraph_init( initial_graph, params_config, log_file )

    // create a new infomap instance 
    val alg = {
        new InfoMap( infomap_config )
    }
    // run infomap algrithm
    val t1 = System.nanoTime 
    val(result_graph,result_commu) = alg( initial_graph, commu, log_file )
    val duration = (System.nanoTime - t1) / 1e9d
    println(s"Running time: $duration s")   
    save_result(result_graph, result_commu, log_file)
  }

    def init_spark( sparkConfig: JsonObj ): (SparkConf,SparkContext) = {
      val master = sparkConfig.getObj("Master").value.toString
      val num_executors = sparkConfig.getObj("num executors").value.toString
      val executor_cores = sparkConfig.getObj("executor cores").value.toString
      val driver_memory = sparkConfig.getObj("driver memory").value.toString
      val executor_memory = sparkConfig.getObj("executor memory").value.toString
      val spark = new SparkConf()
        .setAppName("InfoMap")
        .setMaster(master)
        .set("spark.executor.instances", num_executors )
        .set("spark.executor.cores", executor_cores )
        .set("spark.driver.memory", driver_memory )
        .set("spark.executor.memory", executor_memory )
      val sc = new SparkContext(spark)
      sc.setLogLevel("OFF")
      (spark, sc)
    }

    def init_log( sc: SparkContext, logConfig: JsonObj ): LogFile = new LogFile(
      sc,
      logConfig.getObj("log path").value.toString,
      logConfig.getObj("txt path").value.toString,
      logConfig.getObj("Reduced Json path").value.toString,
      logConfig.getObj("debug").value.toString.toBoolean
    )


    def graph_read( sc: SparkContext, graph_file: String,
    log_file: LogFile ): Graph = {
      log_file.write(s"Reading $graph_file\n",false)
      val graph = GraphReader( sc, graph_file, log_file )
      val vertices = graph.vertices.count
      val edges = graph.edges.count
      log_file.write(
        s"Read in network with $vertices nodes and $edges edges\n",
      false)
      graph
    }

    def subgraph_init( graph: Graph,
    params_config: JsonObj, log_file: LogFile ): Partition = {
      log_file.write(s"Initializing partitioning\n",false)
      val part = Partition.init( graph, params_config, log_file )
      log_file.write(s"Finished initialization calculations\n",false)
      part
    }


    def save_result( graph: Graph, part: Partition, log_file: LogFile )
    : Unit = {
      log_file.write(s"Save final graph\n",false)
      log_file.write(s"with ${part.vertices.count} modules"
        +s" and ${part.edges.count} connections\n",
      false)
      log_file.save( graph, part, false, "" )
    }
    
    // for debugging
    // def log_debug( spark: SparkConf, sc: SparkContext, log_file: LogFile )
    // : Unit = {
    //   val jar = sc.jars.head.split('/').last
    //   val version = jar.split('-').last.split('.').dropRight(1).mkString(".")
    //   log_file.write(s"Running ${sc.appName}, version: $version\n",false)
    //   val jvmHeapSpace = Runtime.getRuntime().maxMemory/1024/1024
    //   log_file.write(
    //     s"Driver memory/Java heap size: $jvmHeapSpace Mb\n",
    //   false)
    //   log_file.write(s"Spark version: ${sc.version}\n",false)
    //   log_file.write(s"Spark configurations:\n",false)
    //   spark.getAll.foreach{ case (x,y) => log_file.write(s"$x: $y\n",false) }
    // }

}