package br.edu.ifpi.jazida.writable;

import org.apache.lucene.search.TopFieldDocs;

public class TopFieldDocsWritable extends AbstractWritable {

	public TopFieldDocsWritable() {
	}

	public TopFieldDocsWritable(TopFieldDocs obj) {
		super(obj);
	}
	
	public TopFieldDocs getTopFieldDocs() {
		return (TopFieldDocs) super.getObject();
	}

}
