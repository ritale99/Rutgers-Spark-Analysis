package com.RUSpark;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import java.util.HashMap;
/* any necessary Java packages here */

public class NetflixGraphGenerate {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		
		//setup spark session
		SparkSession spark = SparkSession
			      .builder()
			      .appName("NetflixGraphGenerate")
			      .getOrCreate();
		
		//setup the schema based on the csv columns
		StructType schema = new StructType()
				  .add("movie_id", "string")
				  .add("customer_id", "int")
				  .add("rating", "double")
				  .add("date", "string");
		
		//set up the dataframe and connect the schema
		Dataset<Row> df = spark.read()
				 .option("mode", "DROPMALFORMED")
				  .schema(schema)
				  .format("com.databricks.spark.csv")
				  .csv(InputPath);
		
		//register the dataFrame as sql temporary view
		df.createOrReplaceTempView("table");
		
		
		Dataset<Row> sqlDf = spark.sql("SELECT T1.movie_id, concat(T1.customer_id,' ',T2.customer_id) FROM table T1, table T2 WHERE t1.movie_id = t2.movie_id AND t1.rating = t2.rating AND t1.customer_id < t2.customer_id;");
		
		//for testing purposes
		//sqlDf.show();
		
		//(customer_id_1,customer_id_2), weight

		
		List<Row> output = sqlDf.collectAsList();
		HashMap<String, Integer> mapCount = new HashMap<String, Integer>();
		
		for(Row row : output) { 
			String out = row.mkString(",");
			String [] rowArr = out.split(",");
			String custIds = rowArr[1];
			
			if(mapCount.containsKey(custIds)) {
				mapCount.put(custIds, mapCount.get(custIds) +1);
			}
			else {
				 mapCount.put(custIds, 1);
			}
			
		}
		
		for(String id: mapCount.keySet()) {
			String ids = id.replace(" ", ",");
			System.out.println( "(" + ids + "), " + mapCount.get(id));
			}
	}

}
