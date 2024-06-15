package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.HashMap;
import java.util.List;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedNews;

/* 
 * This class is used to process the given news articles by concatenating the title and the first 5 paragraphs 
 * of the contents and then processing the concatenated content to get the terms and their counts after stemming 
 * and removing stop words.
 * 
 * Returns the ProcessedNews object which contains the concatenated content, term counts, length 
 * and the original news article.
 */

public class ProcessNewsMap implements MapFunction<NewsArticle,ProcessedNews>{
	private static final long serialVersionUID = 1231167648446238027L;

	// The given text pre-processor object
	private transient TextPreProcessor processor;
	
	// Accumulators to keep track of the number of documents and the total length of the documents
	LongAccumulator doc_count;
	LongAccumulator doc_length;
	
	public ProcessNewsMap() {}
	
	public ProcessNewsMap(LongAccumulator doc_count, LongAccumulator doc_length) {
		this.doc_count = doc_count;
		this.doc_length = doc_length;
	}
	
	@Override
	public ProcessedNews call(NewsArticle article) throws Exception {
		
		if (processor == null) processor = new TextPreProcessor();
		
		List<ContentItem> contents = article.getContents();
		
		// String builder object over String for concatenation effeciency
		StringBuilder builder = new StringBuilder();
		// Concatenating title for the given article
		String title = article.getTitle();
		builder.append(title);
		
		short ctr = 0;
		// Getting the first 5 paragraph if not null and adding to the string builder
		for(ContentItem content : contents) {
			if (ctr == 5) break;
			if (content != null){
				if (content.getSubtype() != null) {
					if (content.getSubtype() == "paragraph") {
						builder.append(content.getContent());
						ctr++;
					}
				}
			}
		}
		
		// Processing the concatenated content to get the terms and their counts.
		String concatenated_content = builder.toString();
		List<String> terms = processor.process(concatenated_content);
		
		int length = terms.size();
		
		doc_count.add(1);
		doc_length.add(length);
		
		// Creating a hash map to store the term counts of the processed news article
		HashMap<String,Integer> term_counts = new HashMap<>();
		
		for (String term : terms) {
			if (term_counts.containsKey(term)) {
				term_counts.put(term, term_counts.get(term) + 1);
			} else {
				term_counts.put(term, 1);
			}
		}
		
		// Return the processed news object
		return new ProcessedNews(concatenated_content,term_counts,length,article);
	}
	
}
