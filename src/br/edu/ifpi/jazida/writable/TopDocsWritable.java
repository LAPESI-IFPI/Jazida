package br.edu.ifpi.jazida.writable;

import org.apache.lucene.search.TopDocs;

public class TopDocsWritable extends AbstractWritable {

	public TopDocsWritable() {
	}

	public TopDocsWritable(TopDocs obj) {
		super(obj);
	}

	public TopDocs getTopDocs() {
		return (TopDocs) super.getObject();
	}
}
