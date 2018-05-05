package edu.bits.ir.indexersearch.models;

import java.util.List;

public class QueryResult {

	boolean isFound;
	List<Term> terms;
	public boolean isFound() {
		return isFound;
	}
	public void setFound(boolean isFound) {
		this.isFound = isFound;
	}
	public List<Term> getTerms() {
		return terms;
	}
	public void setTerms(List<Term> terms) {
		this.terms = terms;
	}
}
