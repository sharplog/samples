package com.github.sharplog.samples.spark.ldasample;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;

import scala.Tuple2;

public class WeiboAnalyzer {

	public static final String weiboFile = "data/weibo/tweets.csv";
	public static final String stopFile = "data/weibo/stopwords.txt";
	
	public static int vocabSize = 100;				// 词汇表长度
	public static int numTopics = 3;				// 主题数量 ，根业务相关
	public static int maxIterations = 100;			// 最大迭代次数
	public static int maxTermsPerTopic = 10;		// 每个主题的前10个词
	public static final Pattern weiboReg = Pattern.compile(
			"^\"(.+)\",(\\d+),(\\d+),(\\d+),\"(.+)\",\"(.+)\",\"(.+)\",\"(.+)\",");
	
	public static void main(String[] args){
		SparkConf sc = new SparkConf().setAppName("WeiboAnalyzer").setMaster("local[2]");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		SQLContext jsql = new SQLContext(jsc);
		
		JavaRDD<String> dataSet = jsc.textFile(weiboFile);
		JavaRDD<String> stopSet = jsc.textFile(stopFile);
		
		// 解析微博内容
		JavaRDD<String> weibo = dataSet.map(new Function<String, String>(){
			@Override
			public String call(String line) throws Exception {
				Matcher m = weiboReg.matcher(line);
				if( m.find() ){
					return m.group(5);	// 微博内容
				}
				return null;
			}
		});
		
		// 去除空内容
		JavaRDD<String> weiboFlt = weibo.filter(new Function<String, Boolean>(){
			@Override
			public Boolean call( String c) throws Exception {
				return null != c && !"".equals(c.trim());
			}
			
		});
		
		// 分词
		JavaRDD<Row> words = weiboFlt.map(new Function<String, Row>(){
			@Override
			public Row call(String content) throws Exception {
				List<String> wl = new ArrayList<String>();
				if( content != null ){
					List<Term> l = HanLP.segment(content);
					for(int i=0; i<l.size(); i++){
						wl.add(l.get(i).word);
					}
				}
				return RowFactory.create(wl);
			}
		});
		
		// 转成DataFrame
		StructType schema = new StructType(new StructField[]{
				new StructField(
				    "words", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
				});
		
		DataFrame wordSet = jsql.createDataFrame(words, schema);
		
		// 去掉停用词
		String[] sa = {};
		StopWordsRemover remover = new StopWordsRemover()
				.setStopWords(stopSet.collect().toArray(sa))
				.setCaseSensitive(false)
				.setInputCol("words")
				.setOutputCol("filtered");
		
		DataFrame wordFlt = remover.transform(wordSet);

		// 生成词汇表
		CountVectorizerModel cvModel = new CountVectorizer()
				.setInputCol("filtered")
				.setOutputCol("features")
				.setVocabSize(vocabSize)
				.fit(wordFlt);
		
		// 为每条微博生成向量
		DataFrame counterVectors = cvModel.transform(wordFlt)
				.select("features");
		
		JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(counterVectors.javaRDD().zipWithIndex().map(
				new Function<Tuple2<Row, Long>, Tuple2<Long, Vector>>(){
					@Override
					public Tuple2<Long, Vector> call(Tuple2<Row, Long> r) throws Exception {
						return new Tuple2<>(r._2(), (Vector)r._1().get(0));
					}
				}
		));
		
		// 进行计算
		DistributedLDAModel ldaModel = (DistributedLDAModel) new LDA()
				.setK(numTopics)
				.setDocConcentration(-1)
				.setTopicConcentration(-1)
				.setMaxIterations(maxIterations)
				.run(corpus);
		
		// 打印每个主题的排名靠前的词汇
		String vocabArray[] = cvModel.vocabulary();
		Tuple2<int[], double[]>[] topicIndices = ldaModel.describeTopics(maxTermsPerTopic);
		for(int i=0; i<topicIndices.length; i++){
			System.out.println("Topic " + i + ": ");
			
			int[] indices = topicIndices[i]._1();
			double[] weights = topicIndices[i]._2();
			for(int j=0; j<indices.length; j++){
				System.out.println(vocabArray[indices[j]] + "\t" + weights[j]);
			}
			System.out.println("================\n");
		}
	}
}
