package edu.bits.ir.indexersearch;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IndexerSearchApplication {
	
	@Autowired
	JavaSparkContext sc;

	public static void main(String[] args) {
		SpringApplication.run(IndexerSearchApplication.class, args);
	}
}
