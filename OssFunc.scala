package com.cloudera.test_oss


import scala.util.matching.Regex
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData.Record
import org.kitesdk.data._

/**
 * Created by ronguerrero on 15-08-11.
 */
object testOssFunc {
  // Function: getKey
  // Input:  t = table name
  //         f = file name
  //         d = timestamp (this is the full timestamp)
  //         mo = measobjldn
  // Output: key in the form of   "<tablename> | RNC | NODEB | .."  Format depends on table HSPA, UMTSCELL
  def getKey(t:String, f:String, d:String, mo:String) : (String,String) = {
    val num_pattern = new Regex("[0-9]{1,}")

    val keyStr = if (t == "HSPA"){
      val rnc_id = (num_pattern findFirstIn f.substring(f.indexOf("RNC"))).mkString
      val nodeb_id = (num_pattern findFirstIn mo.substring(mo.indexOf("Label="))).mkString
      val cell_id = (num_pattern findFirstIn mo.substring(mo.indexOf("CellID="))).mkString
      d.concat("|")
        .concat(rnc_id).concat("|")
        .concat(nodeb_id).concat("|")
        .concat(cell_id)
    } else {
      // TODO - we are assuming UMTSCELL data
      val rnc_id = (num_pattern findFirstIn f.substring(f.indexOf("RNC"))).mkString
      val nodeb_id = (num_pattern findFirstIn mo.substring(mo.indexOf("Label="))).mkString
      val cell_id = (num_pattern findFirstIn mo.substring(mo.indexOf("CellID="))).mkString
      val dest_rnc = (num_pattern findFirstIn mo.substring(mo.indexOf("UMTS:"))).mkString
      val dest_cellid = (num_pattern findFirstIn mo.substring(mo.indexOf("Cell ID"))).mkString
      d.concat("|")
        .concat(rnc_id).concat("|")
        .concat(nodeb_id).concat("|")
        .concat(cell_id).concat("|")
        .concat(dest_rnc).concat("|")
        .concat(dest_cellid).concat("|")
        .concat(f)
    }

    return (t,keyStr)
  }

  // Function: getRec  -- used with parsed XML data
  // Input:  k = key
  //         v = Array of (ColumnName, ColumnValue)
  //         descriptor = record descriptor, we'll deduce available columns from this
  //         builder = record builder object, needed to build the actual avro record
  // Output: (Record, Null)
  def getRec(k:(String, String), v:Array[(String, String)], s:String,
             descriptor : DatasetDescriptor,
             builder : GenericRecordBuilder) : (Record, Null) = {
    val table     = k._1
    val fields    = k._2.split('|')

    val iter = descriptor.getSchema.getFields.iterator()
    while (iter.hasNext)
    {
       val field = iter.next()
       builder.clear(field)
    }

    if (table == "HSPA") {
      // pull key out values
      val beginTime = fields(0)
      val rnc_id    = fields(1).toInt
      val nodeb_id  = fields(2).toInt
      val cell_id   = fields(3).toInt

      // Set the key values in the record builder
      builder.set("begintime", beginTime)
      builder.set("rnc_id"   , rnc_id)
      builder.set("nodeb_id" , nodeb_id)
      builder.set("cell_id"  , cell_id)
    } else {
      // TODO - we are assuming the other table is UMTSCELL
      // pull the key fields out
      val beginTime = fields(0)
      val rnc_id    = fields(1).toInt
      val nodeb_id  = fields(2).toInt
      val cell_id   = fields(3).toInt
      val dest_rnc    = fields(4).toInt
      val dest_cellid   = fields(5).toInt
      val source_file = fields(6)

      // Set the key values in the record builder
      builder.set("begintime", beginTime)
      builder.set("rnc_id"   , rnc_id)
      builder.set("nodeb_id" , nodeb_id)
      builder.set("cell_id"  , cell_id)
      builder.set("dest_rnc"   , dest_rnc)
      builder.set("dest_cellid"  , dest_cellid)
      builder.set("source_file"  , source_file)
    }

    for (col <- v ) {
      if (col._2 != "null") {
        // Set only the fields that exist in this record
        builder.set(col._1,col._2.toDouble)
      }
    }
    val record = builder.build()
    return (record,null)
  }


  // Function: getMstrKey  -- used with MSTR data read from HDFS
  // Input:  t = table name
  //         r = record
  // Output: (key, record)
  def genMstrKey(t:String, r:GenericRecord) : ((String,String),GenericRecord) = {
    val keyStr = if (t == "HSPA") {
      val beginTime = r.get("begintime").toString
      val rnc_id = r.get("rnc_id").toString
      val nodeb_id = r.get("nodeb_id").toString
      val cell_id = r.get("cell_id").toString
      val str = new StringBuilder()
      str.append(beginTime).append("|")
        .append(rnc_id).append("|")
        .append(nodeb_id).append("|")
        .append(cell_id)
      str.toString()
    } else {
      // TODO - we are assuming we are dealing with UMTSCELL
      val beginTime = r.get("begintime").toString
      val rnc_id = r.get("rnc_id").toString
      val nodeb_id = r.get("nodeb_id").toString
      val cell_id = r.get("cell_id").toString
      val dest_rnc = r.get("dest_rnc").toString
      val dest_cellid = r.get("dest_cellid").toString
      val source_file = r.get("source_file").toString
      val str = new StringBuilder()
      str.append(beginTime).append("|")
        .append(rnc_id).append("|")
        .append(nodeb_id).append("|")
        .append(cell_id).append("|")
        .append(dest_rnc).append("|")
        .append(dest_cellid).append("|")
        .append(source_file)
      str.toString()
    }
    return ((t, keyStr),r)
  }
}
