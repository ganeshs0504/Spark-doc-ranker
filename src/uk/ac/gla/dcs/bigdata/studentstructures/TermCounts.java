package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.HashMap;
import java.io.Serializable;

/**
 * Represents the term counts for the entire corpus
 * after reducing the term counts for the given news articles
 * by adding the term counts for the same term.
 */

public class TermCounts implements Serializable{
	private static final long serialVersionUID = -920249627236255175L;
	HashMap<String,Integer> termCount;
	
	public TermCounts() {}
	
	public TermCounts(HashMap<String,Integer> termCount) {
		this.termCount = termCount;
	}
	
	public HashMap<String,Integer> getTermCount(){
		return (HashMap<String, Integer>) this.termCount;
	}
	
	public void setTermCount(HashMap<String,Integer> termCount) {
		this.termCount = termCount;
	}
}
