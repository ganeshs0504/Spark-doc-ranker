package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

/*	
 * This class is used to reduce the document rankings for a given query using the provided text similarity method.
 * This class returns the top 10 news articles for a given query.
 */

public class DocumentRankingsReduceGroups implements ReduceFunction<DocumentRanking>{
	private static final long serialVersionUID = -6943332541383162226L;

	@Override
	public DocumentRanking call(DocumentRanking r1, DocumentRanking r2) throws Exception {
		List<RankedResult> mergedResults = r1.getResults();
		Query query = r1.getQuery();
		
		List<RankedResult> append = r2.getResults();
		
		// Loop to merge the results of the reduce function
		for(RankedResult r : append) {
			boolean valid = true;
			int swap = -1;
			// Check if the article is already in the list and if so, check the similarity of both the articles
			for (int i = 0; i < mergedResults.size();i++) {
				RankedResult result = mergedResults.get(i);
				if (TextDistanceCalculator.similarity(r.getArticle().getTitle(),result.getArticle().getTitle()) < 0.5){
					valid = false;
					if(r.getScore() > result.getScore()) {
						// If the similarity is less than 0.5, then swap the articles
						swap = i;
					}
				}
			}
			// If the article is not in the list, then add it to the list else swap based on the flag
			if (valid) mergedResults.add(r);
			else if(swap != -1) {
				mergedResults.set(swap, r);
			}
		}

		// Sort the list based on the score in descending order for the top articles
		Collections.sort(mergedResults);
		Collections.reverse(mergedResults);
		
		// If the list is greater than 10, then take the top 10 articles
		if (mergedResults.size() > 10) {
			mergedResults = mergedResults.subList(0, 10);
		}
		
		// Return the merged results
		return new DocumentRanking(query,mergedResults);
	}

}
