package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/* 
 * This class is used to map the document rankings to the document ranking object by 
 * removing the Query object from the tuple2 type.
 * 
 * Returns the document ranking object for the final output of the pipeline.
 */

public class TupleToDocumentRankingMap implements MapFunction<Tuple2<Query,DocumentRanking>,DocumentRanking>{
	private static final long serialVersionUID = -3235503045326585449L;

	@Override
	public DocumentRanking call(Tuple2<Query, DocumentRanking> tuple) throws Exception {
		// Return the document ranking object from the input tuple2 type
		return tuple._2();
	}

}
