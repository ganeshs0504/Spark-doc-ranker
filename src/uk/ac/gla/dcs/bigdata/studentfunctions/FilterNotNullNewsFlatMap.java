package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/* 
 * This class is used to filter the news articles which have null title or contents.
 * Returns an iterator of the news articles which have non-null title and contents
 */


public class FilterNotNullNewsFlatMap implements FlatMapFunction<NewsArticle,NewsArticle>{
	
	private static final long serialVersionUID = -4260293140128412243L;
	
	@Override
	public Iterator<NewsArticle> call(NewsArticle news) throws Exception {
		

		List<NewsArticle> newslist;
		
		// Check if the title and contents are not null
		if (news.getTitle() != null && news.getContents() != null) {
			newslist = new ArrayList<NewsArticle>(1);
			// Add the news article to the list
			newslist.add(news);
		// If the title or contents are null, then return an empty list
		} else {
			newslist  = new ArrayList<NewsArticle>(0);
		}
	
		// Return the list of news articles
		return newslist.iterator();
	}
	
	

}
