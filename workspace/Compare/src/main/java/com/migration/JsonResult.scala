package com.migration

import java.sql.DriverManager
import java.sql.Connection
import java.io.FileWriter
import java.io.FileReader
import java.util.Properties;
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.File

object JsonResult {
    def main(args : Array[String]) { 
      println("Hello") 
        // read property file and connect to the database
            var reader=new FileReader("connection.properties");  
            var properties=new Properties();  
            properties.load(reader);  
            var filename =properties.getProperty("filepath");
            var filewrite = new FileWriter(filename);
            Class.forName(properties.getProperty("jdbc.driver")).newInstance();
            var query = properties.getProperty("jdbc.query");
            var jsonoutput = properties.getProperty("jsonoutput");
            var connection:Connection = null
    
	try {
        Class.forName(driver)
        connection = DriverManager.getConnection(properties.getProperty("jdbc.url"),
                    properties.getProperty("jdbc.username"),
                    properties.getProperty("jdbc.password"))
        val statement = connection.createStatement
        val rs = statement.executeQuery(query)
        var rsmd = rs.getMetaData();
        var columnCount = rsmd.getColumnCount();
       
		//write header
       for ( i<-1 to columnCount){
              filewrite.append(rsmd.getColumnName(i));
              if(i<columnCount)
              filewrite.append(',')
          }
       filewrite.append('\n')
       
	   //write data in a file
        while (rs.next) {
               for ( i<-1 to columnCount){
                    filewrite.append(rs.getString(i));
                    if(i<columnCount)
                    filewrite.append(',');
                }
                filewrite.append('\n');
        }
    } catch {
        case e: Exception => e.printStackTrace
    }
    filewrite.flush();
    filewrite.close();
    connection.close
    
	//calling ToJson method
    ToJson(filename,jsonoutput)
  
  } 
    
   def ToJson(input : String,jsonoutput : String) {
     val inputCsvFile = new File(input)

  // if the csv has header, use setUseHeader(true)
  val csvSchema = CsvSchema.builder().setUseHeader(true).build()
  val csvMapper = new CsvMapper()

  // java.util.Map[String, String] identifies they key values type in JSON
  val readAll = csvMapper
    .readerFor(classOf[java.util.Map[String, String]])
    .`with`(csvSchema)
    .readValues(inputCsvFile)
    .readAll()

  val mapper = new ObjectMapper()

  // json return value
  mapper.writerWithDefaultPrettyPrinter().writeValueAsString(readAll)
  
  //print in a file
  val pw = new java.io.PrintWriter(jsonoutput)
  pw.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(readAll))
  pw.close
}
}