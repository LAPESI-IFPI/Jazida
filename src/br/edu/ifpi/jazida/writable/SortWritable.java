package br.edu.ifpi.jazida.writable;

import java.io.Serializable;

import org.apache.lucene.search.Sort;

public class SortWritable extends AbstractWritable {

	public SortWritable() {
	}

	public SortWritable(Serializable obj) {
		super(obj);
	}
	
	public Sort getSort() {
		return (Sort) super.getObject();
	}

}
