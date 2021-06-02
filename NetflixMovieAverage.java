package com.RUSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import java.util.List;
import java.lang.Math;

/* any necessary Java packages here */

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
			      .builder()
			      .appName("NetflixMovieAverage")
			      .getOrCreate();
		
		//setup the schema based on the csv columns
		StructType schema = new StructType()
				 .add("movie_id", "string")
				 .add("customer_id", "string")
		 		 .add("rating", "double")
		 		 .add("date", "string");
		 
		//set up the dataframe and connect the schema
		Dataset<Row> df = spark.read()
				  .option("mode", "DROPMALFORMED")
				  .schema(schema)
				  .format("com.databricks.spark.csv")
				  .csv(InputPath);
		  
		//group by movie_id and calculate average of the rating
		Dataset<Row> dfResult = df.groupBy("movie_id").avg("rating").sort("movie_id");
		
		
		//Dataset<Row> dfResultSorted = dfResult
		//for testing purposes, remove later
		//dfResult.show();
		
		List<Row> output = dfResult.collectAsList();
		
		for(Row row:output) {
			String x = row.mkString(",");
			String rowArr[] = x.split(",");
			String id = rowArr[0];
			double rating = Double.parseDouble(rowArr[1]);
			double roundedRating = Math.round(rating * 100.0)/100.0;
			
			System.out.println(id + " " + roundedRating);
		}
		
		 
	}

}
