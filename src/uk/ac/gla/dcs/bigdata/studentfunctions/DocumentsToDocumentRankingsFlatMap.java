package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedNews;

/* 
 * This class is used to map the processed news to the document rankings for a given query by calculating the dph score.
 * Returns an iterator of the DocumentRanking for the given query which contains dph scores for each article against all the given query terms 
 */

public class DocumentsToDocumentRankingsFlatMap implements FlatMapFunction<ProcessedNews, DocumentRanking>{
    private static final long serialVersionUID = -921519827463523508L;
    Broadcast<List<Query>> broadcastQueryList;
    long docsInCorpus;
    Broadcast<HashMap<String, Integer>> broadcastTermCounts;
    double averageDocumentLength;

    // Constructor to initialize the broadcast values and other variables required for DPH score calculation
    public DocumentsToDocumentRankingsFlatMap(Broadcast<List<Query>> broadcastQueryList, long docsInCorpus, Broadcast<HashMap<String, Integer>> broadcastTermCounts, double averageDocumentLength){
        this.broadcastQueryList = broadcastQueryList;
        this.docsInCorpus = docsInCorpus;
        this.broadcastTermCounts = broadcastTermCounts;
        this.averageDocumentLength = averageDocumentLength;
    }


    @Override
    public Iterator<DocumentRanking> call(ProcessedNews article) throws Exception {

        ArrayList<DocumentRanking> ranking = new ArrayList<>();

        // get all the broadcast values
        List<Query> queryList = broadcastQueryList.value();
        HashMap<String, Integer> termCountCorpus = broadcastTermCounts.value();
        int currDocLength = article.getLength();
        NewsArticle news = article.getArticle();
        
        // loop through all the queries and calculate the dph score for each article
        for(Query query: queryList) {
            ArrayList<RankedResult> results = new ArrayList<>();
            double dph = 0;
            // Inner loop to iterate over query terms in the query object.
            for(String queryTerm: query.getQueryTerms()){
                short termFrequencyInCurrentDocument = article.getDocumentTermCounts().getOrDefault(queryTerm, 0).shortValue();
                int totalTermFrequencyInCorpus = termCountCorpus.getOrDefault(queryTerm, 0).intValue();

                double currDPH = DPHScorer.getDPHScore(
                    termFrequencyInCurrentDocument,
                    totalTermFrequencyInCorpus,
                    currDocLength,
                    averageDocumentLength,
                    docsInCorpus
                );
                // If the dph score is NaN, then set it to 0
                if(Double.isNaN(currDPH)) currDPH = 0;
                
                // Taking the sum of the dph scores if 2 or more query terms are present in the query object.
                dph += currDPH;
            }

            // Adding the ranked result to the results list
            results.add(new RankedResult(news.getId(),news,dph));
            ranking.add(new DocumentRanking(query,results));
        }

        // return obj of DocumentQueryDPHScore which contains a Query and a corresponding RankedResult for the current article
        return ranking.iterator();
    }

    
    
}
