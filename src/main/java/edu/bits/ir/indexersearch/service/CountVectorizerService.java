package edu.bits.ir.indexersearch.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import edu.bits.ir.indexersearch.models.Term;
import scala.collection.mutable.WrappedArray;

@Service
public class CountVectorizerService {

	public Dataset<Row> countVectorize(JavaSparkContext sc, List<List<String>> wordsArray) {
		SparkSession ss = new SparkSession(sc.sc());
		getCvModel(ss, wordsArray).transform(this.getDataSet(ss, wordsArray)).show(true);
		return getCvModel(ss, wordsArray).transform(this.getDataSet(ss, wordsArray));
	}
	
	public CountVectorizerModel getCvModel(JavaSparkContext sc , List<List<String>> wordsArray) {
		SparkSession ss = new SparkSession(sc.sc());
		return getCvModel(ss, wordsArray);
	}
	
	private CountVectorizerModel getCvModel(SparkSession ss , List<List<String>> wordsArray) {
		Dataset<Row> df = this.getDataSet(ss, wordsArray);
		// fit a CountVectorizerModel from the corpus
		CountVectorizerModel cvModel = new CountVectorizer()
		  .setInputCol("text")
		  .setOutputCol("feature")
		  .fit(df)
		  .setBinary(false);
		
		return cvModel;
	}
	
	private Dataset<Row> getDataSet(SparkSession ss, List<List<String>> wordsArray){
		List<Row> data = new ArrayList<Row>();
		for(List<String> l : wordsArray) {
			data.add(RowFactory.create(l));
		}
		
		StructType schema = new StructType(new StructField [] {
		  new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
		});
		
		Dataset<Row> df = ss.createDataFrame(data, schema);
		return df;
	}
	
	public int indexOfTermInVocabulary(String term, CountVectorizerModel model) {
		List<String> vocabularyList = new ArrayList<String>();
		Collections.addAll(vocabularyList, model.vocabulary());
		return vocabularyList.indexOf(term);
	}
	
	public List<Term> findTermOccurences(String term, CountVectorizerModel model, Dataset<Row> countVector) {
		int index = indexOfTermInVocabulary(term, model); 
		List<Term> searchList = new ArrayList<Term>();
		if(index!=-1) {
			List<Row> vectorList = countVector.collectAsList();
			for(int i=0;i < vectorList.size(); i++) {
				Term termObj = new Term();
				termObj.setDocId(i+"");
				termObj.setTerm(term);
				SparseVector sparseVector = (SparseVector)vectorList.get(i).get(1);
				termObj.setDocumentTermFrequency(sparseVector.apply(index)+"");
				searchList.add(termObj);
			}
			return searchList;
			
		}
		else {
			return searchList;
		}
	}
}
