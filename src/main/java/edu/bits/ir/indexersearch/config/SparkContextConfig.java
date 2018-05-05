package edu.bits.ir.indexersearch.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkContextConfig {

	@Bean
	public JavaSparkContext getJavaSparkContext() {
		SparkConf conf = new SparkConf().setAppName("indexer app").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		return sc;
	}
}
