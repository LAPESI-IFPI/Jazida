package br.edu.ifpi.jazida.writable;

import org.apache.lucene.search.Explanation;

public class ExplanationWritable extends AbstractWritable {

	public ExplanationWritable() {
	}

	public ExplanationWritable(Explanation obj) {
		super(obj);
	}

	public Explanation getExplanation(){
		return (Explanation) super.getObject();
	}
}
