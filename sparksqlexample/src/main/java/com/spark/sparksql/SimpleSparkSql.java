package com.spark.sparksql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;


public class SimpleSparkSql {


	public static void main(String[] args) {
		String appName = "Java Spark SQL";
		String master = "local[*]";
		String jsonFile = "src/main/resources/a.json";
		String jsonFileB = "src/main/resources/b.json";
		
		
		SparkSession spark = SparkSession
				  .builder()
				  .master(master)
				  .appName(appName)
				  .getOrCreate();
		
		Dataset<Row> df = spark.read().json(jsonFile);
		//Dataset<Row> dfb = spark.read().json(jsonFileB);
		df.show();
		df.printSchema();
		df.select(col("name"), col("occupation")).show();
		
	}

}