package br.edu.ifpi.jazida.writable;

import br.edu.ifpi.opala.utils.MetaDocument;

public class MetaDocumentWritable extends AbstractWritable {

	public MetaDocumentWritable() {
	}

	public MetaDocumentWritable(MetaDocument metaDocument) {
		super(metaDocument);
	}

	public MetaDocument getMetaDoc() {
		return (MetaDocument) getObject();
	}

}
