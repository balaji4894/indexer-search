package edu.bits.ir.indexersearch.models;

public class Term {

	private String term;
	private String docId;
	private String documentTermFrequency;
	public String getTerm() {
		return term;
	}
	public void setTerm(String term) {
		this.term = term;
	}
	public String getDocId() {
		return docId;
	}
	public void setDocId(String docId) {
		this.docId = docId;
	}
	public String getDocumentTermFrequency() {
		return documentTermFrequency;
	}
	public void setDocumentTermFrequency(String documentTermFrequency) {
		this.documentTermFrequency = documentTermFrequency;
	}
}
