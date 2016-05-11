package com.github.sharplog.samples.spark.ldasample;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CountVectorizerExample {
	public static void main(String[] args) {
	    SparkConf conf = new SparkConf().setAppName("LDA Example").setMaster("local[2]");
	    JavaSparkContext jsc = new JavaSparkContext(conf);
	    SQLContext sqlContext = new SQLContext(jsc);
	    jsc.setLogLevel("WARN");
		
	 // Input data: Each row is a bag of words from a sentence or document.
	    JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
	      RowFactory.create(Arrays.asList("a", "b", "c")),
	      RowFactory.create(Arrays.asList("a", "b", "b", "c", "a")),
	      RowFactory.create(Arrays.asList("a", "b", "b", "c", "d"))
	    ));
	    StructType schema = new StructType(new StructField [] {
	      new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
	    });
	    DataFrame df = sqlContext.createDataFrame(jrdd, schema);

	    // fit a CountVectorizerModel from the corpus
	    CountVectorizerModel cvModel = new CountVectorizer()
	      .setInputCol("text")
	      .setOutputCol("feature")
	      .setVocabSize(4)
	      .setMinDF(1)
	      .fit(df);

	    // alternatively, define CountVectorizerModel with a-priori vocabulary
	    CountVectorizerModel cvm = new CountVectorizerModel(new String[]{"a", "b", "c"})
	      .setInputCol("text")
	      .setOutputCol("feature");

	    cvModel.transform(df).show(false);
	}
}
