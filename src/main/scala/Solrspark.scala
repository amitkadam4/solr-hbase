import java.util

import com.google.gson.JsonParser
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.cloud.{ClusterState, DocCollection, DocRouter, Replica, Slice}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.codehaus.jettison.json.JSONObject
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Solrspark {

case class Properties (
  hdfsPath : String,
  outputTable : String,
  fieldList : String,
  fieldsListSolr : String,
  fieldSeq : String,
  sortField : String,
  collection : String,
  numTasks : Long )

  def main(args: Array[String]): Unit = {
    if (args.length==0){
      println("need at least one parameter")
    }
    val filename = args(0)
    runShardedLoadToPhoenix(filename)
  }

def runShardedLoadToPhoenix(filename : String):Unit={
  val clusterPropsFile = Source.fromFile("cluster_properties.json")
  val clusterProps = clusterPropsFile.mkString
  val parser : JsonParser = new JsonParser
  val clusterObj = parser.parse(clusterProps).asInstanceOf[JSONObject]
  val zkSolrString : String = clusterObj.get("zkSolrString").asInstanceOf[String]
  val zkString : String = clusterObj.get("zkString").asInstanceOf[String]
  clusterPropsFile.close()
  implicit val formats = DefaultFormats
  val json = parse(filename)
  val elements = (json \\ "tableProp").children
  for (acct <- elements){
    val m = acct.extract[Properties]
    establishSolrConnection(zkSolrString,zkString,m.hdfsPath,m.outputTable,m.fieldList,m.sortField,m.fieldSeq,m.sortField,m.collection,m.fieldsListSolr,m.numTasks)
  }
}
  def establishSolrConnection(zkSolrString:String,zkString:String,hdfsPath:String,outputTable:String,fieldList:String,fieldSeq:String,sortField:String,collection:String,fieldsListSolr: String,numTasks:Long): Unit ={
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

//Using CloudSolrClient object to establish connection with solr
	val solr : CloudSolrClient = new CloudSolrClient.Builder().withZkHost(zkSolrString).build()
    solr.setDefaultCollection(collection)
    solr.connect()
    val fieldArr = fieldList.split(regex= ",")
    val state : ClusterState = solr.getZkStateReader.getClusterState
   
   // using docCollection to read active slices from the Solr Collection
    val docCollection = state.getCollectionOrNull(collection)
    val slices : util.Collection[Slice] = docCollection.getActiveSlices

    val arrBuff = new ArrayBuffer[Slice]
	val arrBuffRepl = new ArrayBuffer[Replica]
    slices.foreach(arrBuff+=)
    arrBuff.toParArray.map(slice => exportSolrWriteHDFS(sc, slice.getLeader, state, sortField, collection,fieldsListSolr,hdfsPath))
    
	//Creating DataFrame
	val df = spark.read.option("multiline", true).json(hdfsPath + collection + "*")
	df.printSchema()
    val dfdocs = df.select(functions.explode(df("response.docs"))).toDF(colNames = "docs")
    val dfsave = dfdocs.selectExpr(fieldArr : _*)
    val dfsave_renamecol = dfsave.withColumnRenamed(existingName= "solr_update_tms_s", newName= "solr_update_tms")
	//Overwriting data into Pheonix tableProp
	dfsave_renamecol.repartition(numTasks.toInt)
	.write
	.format(source= "org.apache.phoenix.spark")
	.options(Map("table" -> outputTable, "zkUrl" -> zkString))
	.mode(saveMode.Overwrite).save()
	sc.stop()
  }
  //Reading Solr data and writing to HDFS location
  def exportSolrWriteHDFS(sc : SparkContext, slice : Replica, state : ClusterState, sortField : String, collection:String, fieldsListSolr:String, hdfsPath:String) :Unit = {
  val solrurl = replica.getBaseUrl
  val solr: HttpSolrClient = new HttpSolrClient.Builder(solrurl + "/" + collection).build()

// Running query on Solr
val query: SolrQuery = new SolrQuery
query.setRequestHandler("/export")
query.setSort(sortField, SolrQuery.ORDER.asc)
query.setFields(fieldsListSolr)
query.setQuery("*:*")
query.setRows(2147483647)
val solrRequest: QueryRequest = new QueryRequest(query)
val responseParser = new NoOpResponseParser
responseParser.setWriterType("json")
solr.setParser(responseParser)
var queryResponse: NamedList[AnyRef] = solr.request(solrRequest)
val solrResponse : String = queryResponse.get("response").asInstanceOf[String]

//HDFS Write
val fs = FileSystem.get(sc.hadoopConfiguration)
val output = fs.create(new Path(hdfsPath + replica.getCoreName))
val os = new PrintWriter(output)
os.write(solrResponse)
os.close()
}
}
