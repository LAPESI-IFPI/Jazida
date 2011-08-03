package br.edu.ifpi.jazida.writable;

import org.apache.lucene.search.Query;

public class QueryWritable extends AbstractWritable {

	public QueryWritable() {
	}

	public QueryWritable(Query obj) {
		super(obj);
	}
	
	public Query getQuery() {
		return (Query) super.getObject();
	}

}
