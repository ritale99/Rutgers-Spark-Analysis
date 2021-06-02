package com.RUSpark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
/* any necessary Java packages here */

public class RedditHourImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
			      .builder()
			      .appName("RedditHourImpact")
			      .getOrCreate();
		
		//setup the schema based on the csv columns
		 StructType schema = new StructType()
				 .add("image_id", "string")
				 .add("unixtime", "int")
				 .add("title", "string")
		 		 .add("subreddit", "string")
		 		 .add("upvotes", "integer")
		 		 .add("downvotes", "integer")
		 		 .add("comments", "integer");
		 
		//set up the dataframe and connect the schema
		  Dataset<Row> df = spark.read()
				  .option("mode", "DROPMALFORMED")
				  .schema(schema)
				  .format("com.databricks.spark.csv")
				  .csv(InputPath);
		 
		  
		//UDF to convert to the hour 
		UserDefinedFunction convert = udf((Integer x) -> new java.util.Date((long)x*1000).getHours(), DataTypes.IntegerType);
		Dataset<Row> dfInitial = df.select(col("unixtime"), col("upvotes"), col("downvotes"), col("comments"), convert.apply(col("unixtime")));
		
		//dfInitial.show();
		
		//Dataset<Row> test = df.
		
		//group by unixtime and calculate impact
		Dataset<Row> dfResult = dfInitial.groupBy("UDF(unixtime)").sum("upvotes", "downvotes", "comments");
		
		UserDefinedFunction mode = udf((Long a, Long b, Long c) -> a+b+c, DataTypes.LongType);
		Dataset<Row> sum = dfResult.select(col("UDF(unixtime)"), mode.apply(col("sum(upvotes)"), col("sum(downvotes)"), col("sum(comments)")).as("(a+b+c)"));
		
		
		//for testing purposes, remove after
		//sum.show();
		
		List<Row> output = sum.collectAsList();
	
		TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();
		
		for(Row row:output) {
			String x = row.mkString(",");
			String [] rowArr = x.split(",");
		    String unixTime = rowArr[0];
			String total = rowArr[1];
			
			int unix = Integer.parseInt(unixTime);
			//java.util.Date time = new java.util.Date((long)unix*1000);
			
			//deprecated method, idc 
			//int hour = time.getHours();
			
			treeMap.put(unix, total);
			
			//System.out.println(unixTime + " "+ total);
		}
		
		
		for(Entry<Integer, String> en: treeMap.entrySet()) {
			System.out.println(en.getKey() + " " + en.getValue());
		}
	}

}
