package br.edu.ifpi.jazida.writable;

import br.edu.ifpi.opala.searching.SearchResult;

public class SearchResultWritable extends AbstractWritable {
	
	public SearchResultWritable() {
		//Necessário para serialização
	}
	
	public SearchResultWritable(SearchResult searchResult) {
		super(searchResult);
	}

	public SearchResultWritable(Exception exception) {
		super(exception);
	}

	/**
	 * @return the searchResult
	 */
	public SearchResult getSearchResult() {
		return (SearchResult) super.getObject();
	}
}
