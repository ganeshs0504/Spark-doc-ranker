package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedNews;
import uk.ac.gla.dcs.bigdata.studentstructures.TermCounts;

/* 
 * This class is used to map the processed news to the term counts for the given news article.
 * Returns the TermCounts object which contains the term counts for the given news article.
 */

public class TermCountMap implements MapFunction<ProcessedNews,TermCounts>{
	private static final long serialVersionUID = 19716541287139711L;
	
	@Override
	public TermCounts call(ProcessedNews news) {
		TermCounts counts = new TermCounts(news.getDocumentTermCounts());
		return counts;
	}
}
