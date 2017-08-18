package com.gtja.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.mongodb.spark.MongoSpark;

public class SparkReadData implements Serializable {

	private static final long serialVersionUID = -3783441735592435643L;

	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf().setAppName("ReadData").setMaster("local")
				.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/sparkd.col0");

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

		JavaRDD<String> rdd = sc.textFile("resources/record.txt");

		String schemaString = "";
		for (int i = 0; i <= 99; i++) {
			schemaString += "a" + i + " ";
		}

		schemaString = schemaString.substring(0, schemaString.length() - 1);

		List<StructField> fields = new ArrayList<>();

		for (String fieldName : schemaString.split(" ")) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}

		StructType schema = DataTypes.createStructType(fields);

		JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {

			private static final long serialVersionUID = 215009578063673631L;

			public Row call(String record) throws Exception {
				Object[] fields = record.split("[|]");
				return RowFactory.create(fields);
			}
		});

		DataFrame rowDataFrame = sqlContext.createDataFrame(rowRDD, schema);

		MongoSpark.write(rowDataFrame).mode("overwrite").save();

		System.out.println("saving completed");

		sc.stop();

	}
}
