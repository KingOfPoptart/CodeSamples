package com.sample.scalasparksql


import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object sampleSparkSql {
  
  def main(args: Array[String]) {
    val spark = SparkSession
				  .builder()
				  .master("local")
				  .appName("sparksql")
				  //.config("spark.sql.crossJoin.enabled","true")
				  .getOrCreate()
    
	  
    val ajson = spark.read.json("src/main/resources/a.json")
    val bjson = spark.read.json("src/main/resources/b.json")
    // For implicit conversions like converting RDDs to DataFrames
    //import spark.implicits._
    ajson.show()
    bjson.printSchema()
    
   
    ajson.join(bjson, "name").show
    
  }
}

