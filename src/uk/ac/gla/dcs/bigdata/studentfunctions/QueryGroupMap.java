package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/* 
 * This class is used to map the document rankings to the query object with query as a key.
 * Returns the query object for the given document ranking.
 */

public class QueryGroupMap implements MapFunction<DocumentRanking,Query>{
	private static final long serialVersionUID = 3915233179420743638L;

	@Override
	public Query call(DocumentRanking score) {
		
		return score.getQuery();
	}
}
