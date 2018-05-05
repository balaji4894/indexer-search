package edu.bits.ir.indexersearch.service;


import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

@Service
public class TokenizerService {

	public List<String> tokenize(JavaSparkContext sc, String docData){
		SparkSession ss = new SparkSession(sc.sc());
		java.util.List<Row> data = Arrays.asList(
		  RowFactory.create(0, docData)
		);

		StructType schema = new StructType(new StructField[]{
		  new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
		  new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
		});

		Dataset<Row> sentenceDataFrame = ss.createDataFrame(data, schema);

		RegexTokenizer regexTokenizer = new RegexTokenizer()
		    .setInputCol("sentence")
		    .setOutputCol("words")
		    .setPattern("\\W");  // alternatively .setPattern("\\w+").setGaps(false);

		ss.udf().register(
		  "countTokens", (WrappedArray<?> words) -> words.size(), DataTypes.IntegerType);

		Dataset<Row> regexTokenized = regexTokenizer.transform(sentenceDataFrame);
		regexTokenized.select("sentence", "words")
		    .withColumn("tokens", callUDF("countTokens", col("words")))
		    .show(true);
		WrappedArray<String> wa = (WrappedArray<String>)regexTokenized.collectAsList().get(0).get(2);
		return JavaConversions.seqAsJavaList(wa.toList());
	}
}
