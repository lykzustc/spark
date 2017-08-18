package com.gtja.spark;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.mongodb.spark.MongoSpark;

public class SparkQueryData implements Serializable {

	private static final long serialVersionUID = -547698945003526745L;

	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf().setAppName("ReadData").setMaster("local")
				.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/sparkd.col0")
				.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/sparkd.col1");

		JavaSparkContext sc = new JavaSparkContext(conf);

		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

		DataFrame df = MongoSpark.load(sc).toDF();

		df.registerTempTable("rowtable");

		DataFrame results = sqlContext.sql("select a1 from rowtable where a1 > 0");
		
		MongoSpark.write(results).mode("overwrite").save();;
		
		System.out.println("query end");
		
		sc.stop();

	}

}
