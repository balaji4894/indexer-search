package edu.bits.ir.indexersearch.service;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StopWordsRemover;
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
public class StopWordsRemovalService {
	public List<String> removeStopWords(JavaSparkContext sc,List<String> data) {
		SparkSession ss = new SparkSession(sc.sc());
		List<Row> rowdata = Arrays.asList(RowFactory.create(data));
		StructType schema = new StructType(new StructField[] {
				new StructField("raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()) });
		StopWordsRemover remover = new StopWordsRemover().setInputCol("raw").setOutputCol("filtered");
		Dataset<Row> dataset = ss.createDataFrame(rowdata, schema);
		Dataset<Row> filtereddataset = remover.transform(dataset);
		WrappedArray<String> wa = (WrappedArray<String>)filtereddataset.collectAsList().get(0).get(1);
		return JavaConversions.seqAsJavaList(wa.toList());
	}
}
