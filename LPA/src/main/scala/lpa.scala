// import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.{mutable, Map}
import scala.reflect.ClassTag

object LPA {
    def main(args: Array[String]): Unit = {
        val configFileName = "config.json"
        val config = new JsonReader(configFileName)
        val graphFile = config.getObj("Graph").value.toString
        val( spark, context ) = init_spark( config.getObj("spark configs") )
        val LPAConfig = config.getObj("LPA")
        //val logFile = initLog( context, config.getObj("log") )
        

        // load the input file
        val G = GraphLoader.edgeListFile(context, graphFile)

        // run lpa algorithm for several iterations
        val t1 = System.nanoTime //your first line of the code
        val num_iter = LPAConfig.getObj("num iterations").value.toString.toInt
        val G_result = lpa(G,num_iter)
        val duration = (System.nanoTime - t1) / 1e9d

        println(s"Running time: $duration s")    
        // // debug print result
        // newgraph.vertices.sortBy(_._2).foreach {
        //     case (id, (group)) => println(s"$id is in $group")
        // }
    }

    // initialize Spark context
    def init_spark(sparkConfig: JsonObj): (SparkConf,SparkContext) = {
        val master = sparkConfig.getObj("Master").value.toString
        val numExecutors = sparkConfig.getObj("num executors").value.toString
        val executorCores = sparkConfig.getObj("executor cores").value.toString
        val driverMemory = sparkConfig.getObj("driver memory").value.toString
        val executorMemory = sparkConfig.getObj("executor memory").value.toString
        val spark = new SparkConf()
            .setAppName("LPA")
            .setMaster( master )
            .set( "spark.executor.instances", numExecutors )
            .set( "spark.executor.cores", executorCores )
            .set( "spark.driver.memory", driverMemory )
            .set( "spark.executor.memory", executorMemory )

        val context = new SparkContext(spark)
        // val spark = SparkSession
        //     .builder
        //     .appName(s"${this.getClass.getSimpleName}")
        //     .getOrCreate()
        
        
        //val context = spark.sparkContext
        // val spark = new SparkConf()
        //.setAppName("InfoFlow")
        // .setAppName("LPA")
        // .setMaster( master )
        // .set( "spark.executor.instances", numExecutors )
        // .set( "spark.executor.cores", executorCores )
        // .set( "spark.driver.memory", driverMemory )
        // .set( "spark.executor.memory", executorMemory )
        //val sc = new SparkContext(spark)
        // sc.setLogLevel("OFF")
        ( spark, context )
    }

    def lpa[VD, ED: ClassTag](g: Graph[VD, ED], max_round: Int): Graph[VertexId, ED] = {

        require(max_round > 0, s"max_round is ${max_round}")

        val lpaGraph = g.mapVertices{ case (vid, _) => vid }

        /* send msg to other nodes */
        def msg_send(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = 
        {
            Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
        }

        def msg_merge(cnt1: Map[VertexId, Long], cnt2: Map[VertexId, Long]): Map[VertexId, Long] = {
            val map = mutable.Map[VertexId, Long]()
            (cnt1.keySet ++ cnt2.keySet).foreach { 
                i =>
                val cntval = cnt1.getOrElse(i, 0L) + cnt2.getOrElse(i, 0L)
                //val cnt2_val = cnt2.getOrElse(i, 0L)

                map.put(i, cntval)
            }
            map
        }

        def vprogram(vid: VertexId, attr: Long, msg: Map[VertexId, Long]): VertexId = {if (msg.isEmpty) attr else msg.maxBy(_._2)._1}
            val init_msg = Map[VertexId, Long]()

            Pregel(lpaGraph,init_msg,maxIterations=max_round)(
            vprog = vprogram,
            sendMsg = msg_send,
            mergeMsg = msg_merge
        )
    }

}
