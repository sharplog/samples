package com.github.sharplog.samples.spark.ldasample;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Word2VecExample {
	public static void main(String[] args) {
	    SparkConf conf = new SparkConf().setAppName("LDA Example").setMaster("local[2]");
	    JavaSparkContext jsc = new JavaSparkContext(conf);
	    SQLContext sqlContext = new SQLContext(jsc);
	    jsc.setLogLevel("WARN");
		
		// Input data: Each row is a bag of words from a sentence or document.
		JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
		  RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
		  RowFactory.create(Arrays.asList("Hello I heard about Spark".split(" "))),
		  RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
		  RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
		));
		StructType schema = new StructType(new StructField[]{
		  new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
		});
		DataFrame documentDF = sqlContext.createDataFrame(jrdd, schema);

		// Learn a mapping from words to Vectors.
		Word2Vec word2Vec = new Word2Vec()
		  .setInputCol("text")
		  .setOutputCol("result")
		  .setVectorSize(4)
		  .setMinCount(0);
		Word2VecModel model = word2Vec.fit(documentDF);
		DataFrame result = model.transform(documentDF);
		for (Row r : documentDF.select("text").take(4)) {
			  System.out.println(r);
			}
		for (Row r : result.select("result").take(4)) {
			  System.out.println(r);
			}
		
		
	}
}
