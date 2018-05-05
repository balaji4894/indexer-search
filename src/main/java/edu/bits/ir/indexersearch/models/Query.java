package edu.bits.ir.indexersearch.models;

public class Query {
	private String[] terms;
	private String queryType;
	public String[] getTerms() {
		return terms;
	}
	public void setTerms(String[] terms) {
		this.terms = terms;
	}
	public String getQueryType() {
		return queryType;
	}
	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}
}
