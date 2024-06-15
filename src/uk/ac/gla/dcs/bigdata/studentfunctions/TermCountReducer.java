package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.HashMap;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.TermCounts;

/* 
 * This class is used to reduce the term counts for the given news articles
 *  by adding the term counts for the same term.
 * 
 * Returns the TermCounts object which contains the term counts for the 
 * entire corpus
 */

public class TermCountReducer implements ReduceFunction<TermCounts>{
	private static final long serialVersionUID = 1161750991658094748L;

	@Override
	public TermCounts call(TermCounts c1,TermCounts c2) {
		HashMap<String,Integer> counts = c1.getTermCount();
		
		// Loop through the term counts and add the counts for the same term
		for (HashMap.Entry<String,Integer> count : c2.getTermCount().entrySet()) {
			String key = count.getKey();
            Integer value = count.getValue();
            
			if (counts.containsKey(key)) {
				counts.put(key, counts.get(key) + value);
			} else {
				counts.put(key, value);
			}
		}
		
		// Return the term counts for the entire corpus
		return new TermCounts(counts);
	}
}
