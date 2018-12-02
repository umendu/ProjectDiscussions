import scala.xml.XML

import org.apache.spark.{SparkContext, SparkConf}

import org.kitesdk.data._
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import com.cloudera.sparkavro.MyKryoRegistrator

object testOssLoadXml {
  def main(args: Array[String]) {

    val p_tableName = args(0)
    val sparkConf = new SparkConf().setAppName("test OSS Load XML - ".concat(p_tableName))
    MyKryoRegistrator.register(sparkConf)
    val sc = new SparkContext(sparkConf)

    val p_curPartition = args(1)
    val p_prevPartition = args(2)
    val p_inputDir = args(3)
    val p_schema = args(4)
    val p_curDay = args(5)
    val p_prevDay = args(6)
    val p_curOutput = args(7)
    val p_prevOutput =  args(8)
    val p_numInputPartitions = args(9).toInt
    val p_numReducers = args(10).toInt
    val p_omgroup_lookup = args(11)
    val p_col_lookup = args(12)

    // initialize all lookup tables
    val omgroup_lookup = sc.textFile(p_omgroup_lookup)
      .map (line => {
      val fields = line.split(",")
      (fields(0).toInt,fields(1).toString)
    })

    // Collect the OM Group data locally and create into a map
    val hm=omgroup_lookup.collect
    val m = Map(hm:_*)

    // Column lookup table and put into a map
    val col_lookup = sc.textFile(p_col_lookup).map(line => {val fields = line.split(",")
      (fields(0).toString,fields(1).toInt) })
    val hn = col_lookup.collect
    val n = Map(hn:_*)

    // broadcast shared variables
    val b_omgroup = sc.broadcast(m)
    val b_collist = sc.broadcast(n)
    val b_curDay = sc.broadcast(p_curDay)
    val b_prevDay = sc.broadcast(p_prevDay)
    val b_schema = sc.broadcast(p_schema)


    // Function: parseXML
    // Key : (String, String) --> ("HSPA", "2015-07-05 13:45:01|more|junk|here")
    // Val : [ (String, String), (String, String), ... , (String, String) ]
    //    -> [ ("colname", "value1"), ("colname2", "value2") ... ]
    def parseXML (f:String, s: String) : scala.collection.immutable.Seq[((String, String), Array[(String, String)])] = {
      val xml = XML.loadString(s.replaceAll("\n",""))
      val beginTime = ( xml \\ "measCollecFile" \\ "fileHeader" \\ "measCollec" \ "@beginTime").text.replaceFirst("T"," ").substring(0,19)
      // Filter content by cur and previous day
      
      println("BeginTime -" + beginTime)
      
      if ( (beginTime.substring(0,10) == b_curDay.value) || (beginTime.substring(0,10) == b_prevDay.value) )
      {
        val measinfos = ( xml \\ "measCollecFile" \ "measData" \ "measInfo")
        val mi = for { measinfo <- measinfos
                       measinfoid = ( measinfo \ "@measInfoId").text.toInt
                       // get the meas types list
                       measTypes = (measinfo \\ "measTypes").text.split(' ').map(x=>"C".concat(x))
                       // get the measValue list
                       measValues = (measinfo \\ "measValue")
                       measobjresults = if (b_omgroup.value.get(measinfoid).isEmpty) List()
                       else for { item <-measValues
                                  table = b_omgroup.value.get(measinfoid).get
                                  measObjLdn = (item \ "@measObjLdn").text
                                  measresult = (item \ "measResults").text.replaceAll("NIL","null")
                                  measresultList = measresult.split(' ')
                                  key = testOssFunc.getKey(table, f, beginTime, measObjLdn)
                                  cols = measTypes zip measresultList
                                  filterCols =  cols.filter(x => !b_collist.value.get(table.concat("|").concat(x._1)).isEmpty)
                       } yield (key,filterCols)
        } yield measobjresults
        mi.flatMap(x=>x)
      }
      else
      {
        List()
      }
    }

    // Instantiate a configuration object. We need this to read/write to HDFS
    val conf = new Configuration()

    // Load the the XML files, and send one to a spark task
    val data = sc.wholeTextFiles(p_inputDir, p_numInputPartitions)
                    .flatMap({case (filename, content) => parseXML(filename, content)})
                    .reduceByKey(_++_, p_numReducers)

    // We now have elements looking like:
    // data = [    x._1          x._2                                 y
    //           ("HSPA", "2015-07-05 13:45:01|more|junk|here"), [ (,), (,), (,) ]
    //           ("HSPA", "2015-07-05 13:47:32|some|junk|here"), [ (String,String), [], [] ]
    //        ]


    // We have an RDD that has data for the current processing data, and previous processing date
    // We need to split them up into two separate RDDs
    val curData  = data.filter{ case(x,y) => x._2.substring(0,10) == b_curDay.value }
    val prevData = data.filter{ case(x,y) => x._2.substring(0,10) == b_prevDay.value }


    // Build a data descriptor for this dataset - contains all the columns
    // we could ever want from this dataset.
    val descriptor = new DatasetDescriptor.Builder().schemaUri(p_schema)
      .compressionType(CompressionType.Snappy).format(Formats.PARQUET).build()

    // Do not write anything out if we didn't end up parsing out any data from the XML
    if (!curData.isEmpty )
    {
      // Call the Kite API to create an empty dataset in HDFS
      val curHspa = Datasets
        .create("dataset:".concat(p_curOutput), descriptor, classOf[GenericRecord])
        .asInstanceOf[Dataset[GenericRecord]]

      // We now want to populate this dataset,  modify our Configuration object to point to this dataset
      DatasetKeyOutputFormat.configure(conf)
        .writeTo("dataset:".concat(p_curOutput))
        .withType(classOf[GenericRecord])

      val curWrite = curData.mapPartitions{  z=>
        // this block gets invoked within each executor!!!
        // we only want to instantiate the Kite Java Objects once, and pass into the map function
        // otherwise, the instantiation would occur per record - UMTSCELL = 25 million times!!!!!!!!!
        val descriptor = new DatasetDescriptor.Builder().schemaUri(p_schema).compressionType(CompressionType.Snappy).format(Formats.PARQUET).build()
        val builder = new GenericRecordBuilder(descriptor.getSchema)
        //
        z.map{case (x, y) => testOssFunc.getRec(x, y, p_schema, descriptor, builder)
        }
      }

      // save the RDD to disk as a parquet file
      curWrite.saveAsNewAPIHadoopFile("dummy", classOf[GenericRecord],
        classOf[Void] ,classOf[DatasetKeyOutputFormat[GenericRecord]], conf)
    }

    // The code below handles the previous processing days data
    //  TODO -  This should be refactored!!!!!!
    if (!prevData.isEmpty )
    {
      // Call the Kite API to create an empty dataset in HDFS
      val prevHspa = Datasets
        .create("dataset:".concat(p_prevOutput), descriptor, classOf[GenericRecord])
        .asInstanceOf[Dataset[GenericRecord]]

      // We now want to populate this dataset,  modify our Configuration object to point to this dataset
      DatasetKeyOutputFormat.configure(conf)
        .writeTo("dataset:".concat(p_prevOutput))
        .withType(classOf[GenericRecord])

      val prevWrite = prevData.mapPartitions { z=>
        // this block gets invoked within each executor!!!
        // we only want to instantiate the Kite Java Objects once, and pass into the map function
        // otherwise, the instantiation would occur per record - UMTSCELL = 25 million times!!!!!!!!!
        val descriptor = new DatasetDescriptor.Builder().schemaUri(p_schema).compressionType(CompressionType.Snappy).format(Formats.PARQUET).build()
        val builder = new GenericRecordBuilder(descriptor.getSchema)
        z.map { case (x,y) => testOssFunc.getRec(x, y, b_schema.value, descriptor, builder)}
      }

      // save the RDD to disk as a parquet file
      prevWrite.saveAsNewAPIHadoopFile("dummy", classOf[GenericRecord], classOf[Void],
        classOf[DatasetKeyOutputFormat[GenericRecord]], conf)
    }
  }
}

