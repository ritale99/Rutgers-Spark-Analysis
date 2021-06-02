package com.RUSpark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

import static org.apache.spark.sql.functions.udf;

import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;

import static org.apache.spark.sql.functions.col;

/* any necessary Java packages here */

public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		 SparkSession spark = SparkSession
			      .builder()
			      .appName("RedditPhotoImpact")
			      .getOrCreate();
		 
		 //setup the schema based on the csv columns
		 StructType schema = new StructType()
				 .add("image_id", "string")
				 .add("unixtime", "string")
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
		 
		 //Impact(imageID) = upvotes(post with imageID) + downvotes(post with imageID) + comments(post with imageId)  
		 Dataset<Row> dfResult = df.groupBy("image_id").sum("upvotes", "downvotes", "comments");
		 
		 UserDefinedFunction mode = udf((Long a, Long b, Long c) -> a+b+c, DataTypes.LongType);
		 Dataset<Row> sum = dfResult.select(col("image_id"), mode.apply(col("sum(upvotes)"), col("sum(downvotes)"), col("sum(comments)")).as("(a+b+c)"));
		 
		 //For testing purposes, remove after
		 //sum.show();
		 
		 List<Row> output = sum.collectAsList();
		
		 TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();
		 
		 for(Row row : output) {
			 String x = row.mkString(",");
			 String [] rowArr = x.split(",");
			 int id = Integer.parseInt(rowArr[0]);
			 String total = rowArr[1];
			 
			 treeMap.put(id, total);
			 
			 //System.out.println(id + " "+ total);
		 }
		 
		 for(Entry<Integer, String> en: treeMap.entrySet()) {
				System.out.println(en.getKey() + " " + en.getValue());
			}
		  
	}

}
