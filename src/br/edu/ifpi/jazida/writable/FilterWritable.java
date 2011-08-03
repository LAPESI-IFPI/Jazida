package br.edu.ifpi.jazida.writable;

import org.apache.lucene.search.Filter;

public class FilterWritable extends AbstractWritable {

	public FilterWritable() {
	}

	public FilterWritable(Filter obj) {
		super(obj);
	}
	
	public Filter getFilter() {
		return (Filter) super.getObject();
	}

}
