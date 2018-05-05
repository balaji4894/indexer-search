package edu.bits.ir.indexersearch.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import edu.bits.ir.indexersearch.models.Query;
import edu.bits.ir.indexersearch.models.QueryResult;
import edu.bits.ir.indexersearch.models.Term;

@Service
public class QueryService {
	
	@Autowired
	CountVectorizerService csv;

	public HashMap<String, Object> queryTerms(Query query, JavaSparkContext jsc, List<List<String>> wordsArray){
		HashMap<String, Object> returnData = new HashMap<String, Object>();
		HashMap<String, QueryResult> result = this.queryConjenctiveTerms(query.getTerms(), jsc, wordsArray);
		returnData.put("isFound", new Boolean(true));
		result.forEach((key, value)->{
			if(!value.isFound()) {
				returnData.put("isFound", new Boolean(false));
			}
		});
		switch(query.getQueryType()) {
		case "CONJUNCTIVE":
						
			if((Boolean)returnData.get("isFound")) {
				HashMap<String, List<Boolean>> commonDocs = findCommonDocumentIds(query, result, 6);
				commonDocs.forEach((docId, booleanList)->{
					Boolean targetValue = booleanList.size()>0 ? true:false;
					//booleanList.forEach(val->doConjunction(val, targetValue));
					for(int i=0; i< booleanList.size();i++) {
						targetValue = targetValue && booleanList.get(i);
					}
					returnData.put(docId, targetValue);
				});
			}
			break;
		
		case "DISJUNCTIVE":
						
			if((Boolean)returnData.get("isFound")) {
				HashMap<String, List<Boolean>> commonDocs = findCommonDocumentIds(query, result, 6);
				commonDocs.forEach((docId, booleanList)->{
					Boolean targetValue = false;
					//booleanList.forEach(val->doConjunction(val, targetValue));
					for(int i=0; i< booleanList.size();i++) {
						targetValue = targetValue || booleanList.get(i);
					}
					returnData.put(docId, targetValue);
				});
			}
			break;
		}
		return returnData;
	}
	
	private HashMap<String, QueryResult>queryConjenctiveTerms(String[] terms, JavaSparkContext jsc, List<List<String>> wordsArray){
		HashMap<String, QueryResult> returnData = new HashMap<String, QueryResult>();
		for(String term : terms) {
			QueryResult result = new QueryResult();
			List<Term> queriedterms = csv.findTermOccurences(term, csv.getCvModel(jsc, wordsArray), csv.countVectorize(jsc, wordsArray));
			if(queriedterms.size()>0)
				result.setFound(true);
			else
				result.setFound(false);
			result.setTerms(queriedterms);
			returnData.put(term, result);
		}
		return returnData;
	}
	
	private HashMap<String, List<Boolean>> findCommonDocumentIds(Query query, HashMap<String, QueryResult> result, int totalDocuments) {
		HashMap<String, List<Term>> docIdToTermMap = new HashMap<String, List<Term>>();// This will have all terms in doc id
		HashMap<String, List<Boolean>> docIdToQueryTermFound = new HashMap<String, List<Boolean>>();
		
		result.forEach((key, value) -> {
			value.getTerms().forEach(term ->{
				if(docIdToTermMap.containsKey(term.getDocId())) {
					List<Term> termsByDocId = docIdToTermMap.get(term.getDocId());
					termsByDocId.add(term);
					docIdToTermMap.put(term.getDocId(), termsByDocId);
				}
				else {
					List<Term> termsByDocId = new ArrayList<Term>();
					termsByDocId.add(term);
					docIdToTermMap.put(term.getDocId(), termsByDocId);
				}
			});
		});
		
		
		for(int i=0; i < query.getTerms().length;i++) {
			String targetTerm = query.getTerms()[i];
			docIdToTermMap.forEach((key, value)->computDocIdToQueryTermFound(value, key, targetTerm,docIdToQueryTermFound));
		}
		
		return docIdToQueryTermFound;
	}
	
	private void computDocIdToQueryTermFound(List<Term> termList,String docId, String targetTerm, HashMap<String, List<Boolean>> docIdToQueryTermFound) {
		termList.forEach(term -> computDocIdToQueryTermFound(term, targetTerm, docIdToQueryTermFound, docId));
	}
	
	private void computDocIdToQueryTermFound(Term term, String targetTerm, HashMap<String, List<Boolean>> docIdToQueryTermFound, String docId) {
		if(term.getTerm().equals(targetTerm)) {
			if(Double.parseDouble(term.getDocumentTermFrequency())>0) {
				if(docIdToQueryTermFound.containsKey(docId)) {
					List<Boolean> list = docIdToQueryTermFound.get(docId);
					list.add(true);
					docIdToQueryTermFound.put(docId, list);
				}
				else {
					List<Boolean> list = new ArrayList<Boolean>();
					list.add(true);
					docIdToQueryTermFound.put(docId, list);
				}
			}
			else {
				if(docIdToQueryTermFound.containsKey(docId)) {
					List<Boolean> list = docIdToQueryTermFound.get(docId);
					list.add(false);
					docIdToQueryTermFound.put(docId, list);
				}
				else {
					List<Boolean> list = new ArrayList<Boolean>();
					list.add(false);
					docIdToQueryTermFound.put(docId, list);
				}
			}
			
		}
	}
}
