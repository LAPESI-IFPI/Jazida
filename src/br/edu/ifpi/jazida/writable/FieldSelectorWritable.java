package br.edu.ifpi.jazida.writable;

import org.apache.lucene.document.FieldSelector;

public class FieldSelectorWritable extends AbstractWritable {
	public FieldSelectorWritable() {
	}
	public FieldSelectorWritable(FieldSelector field){
		super(field);
	}
	public FieldSelector getFieldSelector() {
		return (FieldSelector) super.getObject();
	}
}
