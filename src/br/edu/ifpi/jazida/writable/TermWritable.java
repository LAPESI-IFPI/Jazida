package br.edu.ifpi.jazida.writable;

import org.apache.lucene.index.Term;

public class TermWritable extends AbstractWritable {
	public TermWritable() {
	}
	public TermWritable(Term term) {
		super(term);
	}
	public Term getTerm() {
		return (Term) super.getObject();
	}
}
