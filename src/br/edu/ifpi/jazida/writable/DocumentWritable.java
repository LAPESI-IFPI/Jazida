package br.edu.ifpi.jazida.writable;

import org.apache.lucene.document.Document;

public class DocumentWritable extends AbstractWritable {
	public DocumentWritable() {
		super(null);
	}
	public DocumentWritable(Document document) {
		super(document);
	}
	public Document getDocument() {
		return (Document) super.getObject();
	}
}
