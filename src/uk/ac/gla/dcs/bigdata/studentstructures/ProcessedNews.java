package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.HashMap;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;


/**
 * Represents a single news article 
 * after PreProcessing, tokenizing, 
 * and removing unnecessary fields
 * @author Zoltan
 *
 */
public class ProcessedNews implements Serializable {
	private static final long serialVersionUID = -4374873213596984354L;

	String concatenatedContent;
	NewsArticle article;
	int length;
	HashMap<String,Integer> documentTermCounts;
	
	public ProcessedNews() {}
	
	public ProcessedNews( String concatenatedContent,HashMap<String,Integer> terms,int length, NewsArticle article) {
		this.concatenatedContent = concatenatedContent;
		this.documentTermCounts = terms;
		this.length = length;
		this.article = article;
	}
	
	public NewsArticle getArticle() {
		return article;
	}
	
	public void setArticle(NewsArticle article) {
		this.article = article;
	}
	
	public String getConcatenatedContent() {
		return concatenatedContent;
	}
	
	public void setConcatenatedContet(String concatenatedContent) {
		this.concatenatedContent = concatenatedContent;
	}
	
	public HashMap<String,Integer> getDocumentTermCounts() {
		return documentTermCounts;
	}

	public void setDocumentTermCounts(HashMap<String,Integer>  documentTermCounts) {
		this.documentTermCounts = documentTermCounts;
	}
	
	public int getLength() {
		return length;
	}
	
	public void setLength(int length) {
		this.length = length;
	}
}
