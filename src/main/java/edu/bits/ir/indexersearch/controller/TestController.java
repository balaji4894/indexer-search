package edu.bits.ir.indexersearch.controller;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import edu.bits.ir.indexersearch.models.Query;
import edu.bits.ir.indexersearch.models.Term;
import edu.bits.ir.indexersearch.service.CountVectorizerService;
import edu.bits.ir.indexersearch.service.QueryService;
import edu.bits.ir.indexersearch.service.StopWordsRemovalService;
import edu.bits.ir.indexersearch.service.TokenizerService;


@RestController
public class TestController {
	
	@Autowired
	JavaSparkContext sc;
	
	@Autowired
	TokenizerService ts;
	
	@Autowired
	StopWordsRemovalService swr;
	
	@Autowired
	CountVectorizerService cvs;
	
	@Autowired
	QueryService qs;
	
	@GetMapping("/test")
	public String test() {
		return sc.appName();
	}
	
	@GetMapping("/")
	public String root() {
		return "Welcome to "+sc.appName();
	}
	
	@GetMapping("/search/{term}")
	public List<Term> countvectorizer(@PathVariable("term") String term) throws IOException {
		ClassLoader classLoader = getClass().getClassLoader();
		List<List<String>> data = new ArrayList<List<String>>();
		for(int i=1; i<7; i++) {
			FileInputStream fis = new FileInputStream(ResourceUtils.getFile("classpath:data/doc" +i+".txt"));
			Scanner in = new Scanner(fis);
			StringBuffer sb = new StringBuffer();
			while(in.hasNext()) {
				sb.append(in.nextLine());
			}
			data.add(swr.removeStopWords(sc,ts.tokenize(sc, sb.toString())));
			in.close();
		}		
		//data.add(swr.removeStopWords(sc,ts.tokenize(sc, "I am trying again to check if count vectorizer adds same index to same words in different documents")));
		
		return cvs.findTermOccurences(term, cvs.getCvModel(sc, data), cvs.countVectorize(sc, data));
	}
	
	@PostMapping("/query")
	public HashMap<String, Object> queryTerms(@RequestBody Query query) throws IOException {
		ClassLoader classLoader = getClass().getClassLoader();
		List<List<String>> data = new ArrayList<List<String>>();
		for(int i=1; i<7; i++) {
			
			FileInputStream fis = new FileInputStream(ResourceUtils.getFile("classpath:data/doc" +i+".txt"));
			Scanner in = new Scanner(fis);
			StringBuffer sb = new StringBuffer();
			while(in.hasNext()) {
				sb.append(in.nextLine());
			}
			data.add(swr.removeStopWords(sc,ts.tokenize(sc, sb.toString())));
			in.close();
		}		
		//data.add(swr.removeStopWords(sc,ts.tokenize(sc, "I am trying again to check if count vectorizer adds same index to same words in different documents")));
		
		//return cvs.findTermOccurences(term, cvs.getCvModel(sc, data), cvs.countVectorize(sc, data));
		/*Query query = new Query();
		query.setQueryType("CONJUNCTIVE");
		query.setTerms(new String[] {"stanford", "hospital"});*/
		System.out.println();
		return qs.queryTerms(query, sc, data);
	}
	
}
