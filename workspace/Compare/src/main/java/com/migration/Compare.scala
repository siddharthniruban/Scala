package Migration

import org.apache.log4j.Logger 
import org.apache.log4j. Level 
import java.io.FileInputStream 
import java.util. Properties 
import java.io._ 
import org.apache.spark.SparkConf 
import org.apache.spark.SparkContext 
import org.apache.spark.sql._ 
import org.joda.time. DateTime
import org.apache.spark.sql.hive.HiveContext

object Compare {
val logger: Logger = Logger.getLogger (Compare.getClass) 
def Filewriter(input : Array[org.apache.spark.sql.Row], filename : String)={
	val bw = new BufferedWriter (new FileWriter (s"$filename", false)) 
	for(i <- 0 to input.size-1) { 	
		var resid = input (i).getAs [String] (0)
		bw.write (resid+"\n")
		bw.close
	}
	
	
def main (args: Array[String]): Unit = {

	if (args.length != 2) { 
	sys.error("Invalid Arguments passed...!"+
	"Usage: Comparsion <PropertyFilepath> <outputpath>") 
	System.exit (1) 
	} else {
	logger.info:("Parameters passed are: S{args.toList.toString())") 
	logger.info (g"Data Comparsion job started at: ${DateTime.Now ()"}")

	//println("Hello, world!) 
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger ("akka").setLevel(Level.OFF)  
	
	//declaring global variable
	val conf = new Sparkconf ().setAppName ("appName")
	val sc = new SparkContext(conf)
	val sqlContext= new org.apache.spark.sql.SQLContext(sc) 
	import sqlContext.implicits.
	val hiveContext = new HiveContext(sc) 
	import hiveContext.implicits._

	//reading the property file
	val props: Properties = new Properties 
	var filepath = args (0)
	var outputpath=args (1) 
	props.load (new FileInputStream (filepath))
	
	//set property value to variables
	var a = props.getProperty("database") 
	var db1 = a.split(":") (0) var db2 = a.split(":") (1)

	var b = props.getProperty("tablename") 
	var tbl1 = b.split(":") (0)
	var tb12 = b.split(":") (1)

	var c = props.getProperty ("partition") 
	var part1 = c.split(":") (0) 
	var part2 = c.split(":") (1)
	
	var d = props.getProperty ("partition column") 
	var part coll= d.split(":") (0)
	var part co12 = d.split(":") (1)
	
	var partstr = new StringBuilder 
	partstr.append("where a Spart1= $part coll'lland b.part2= $part col2")

	var e= props.getProperty ("keycols") 
	var key_col1 = e.split(":")(0) 
	var key col2 = e.split(":")(1)
		
	var f = props.getProperty("columns") 
	var columns = f.split(",")
	var casestr= new StringBuilder 
	columns.foreach(col=> var d = col.split(":");(casestr.append(", case when a.").append(d(0)).append("=b.").append(d(1)).append( END as ").append(d(0)).append("_comp"))}

	//getting records that match in both tables 
	var inputstr = s"select a.$key col1 $casestr from $db1.$tbl1 a join $db2.$tb12 b on (a.$key coll=b.$key_co12) $partstr

	//executing the query and register it as table
	var partial result = hiveContext.sql (s"$inputstr".stripMargin)
	partial_result.registerTemptable ("resultTable").
	
	//dynamically generate the MATCHING column criteria for all matching records and write to file
	var op_comp_match = new StringBuilder ("Where ").
	columns.foreach{col=> var d = col.split(":");(op_comp_match.append(d(0)).append("_comp='MATCH' and ")} 
	op_comp_match.setLength(op_comp. match.length() -4)
	var outputstr1 =s"""select 
	a.$key coll from resultTable a Sop_comp_match"""

	var op = hiveContext.sql (s"Şoutputstr1.stripMargin).collect 
	Filewriter (op, s"$outputpath/All Matching records") 
	1ogger.info (s"${DateTime.now()} :All matching records found in path : $outputpath/Al1 Matching records")

	//dynamically generate the NONMATCH column criteria for all matching records
	var op_comp_nonmatch = new StringBuilder("Where ")
	columns.foreach{col=> var d = col.split(":");(op_comp_nonmatch.append(d(0)).append("comp='NON MATCH' and"))} 
	op_comp_nonmatch.setLength(op_comp_nonmatch.length()-4)

	var outputstr2 =s"""select a. $key coll
	from resultTable
	Sop comp_nonmatch"""
	op = hiveContext.sql(s"$outputstr2".stripMargin).collect 
	Filewriter (op, "$outputpath/Non matching_cols") 
	logger.info (3"${DateTime.now()} :All matching records with nonmatching column values :$outputpath/Non matching_cols")

	//Get Records contains in 1st table not in 2nd table and write to a file
	partstr = new StringBuilder partstr.append("where ").append(s"a.$part1='$part coll' and b. Spart2='$part col2' and ").append(s"b.$key co12 is null")
	inputstr =s"""
	select a.$key_coll from $db1.$tbll a left outer join $db2.$tbl2 b on (a.$key_coll=b.$key_col2) Spartstr
	op = hiveContext.sql(s"$inputstr".stripMargin).collect 
	Filewriter (op, s"$outputpath/1st contains 2nd nonMatch")

	//Get the records contains in 2nd table not in 1st table and write to a file
	inputstr =s""" select a. $key coll from $db2. $tb12 a left outer join $db1.$tbll b on (a. $key_col2=b.$key_coll) $partstr""" 
	op = hiveContext.sql (s"$inputstr".stripMargin).collect
	Filewriter (op, s"$outputpath/2nd_contains1st_nonMatch")
	logger.info (s"${DateTime.now()} :Non matching records in 1st table found in path : $outputpath/2nd_contains_1st_nonmatch");

	}
}
