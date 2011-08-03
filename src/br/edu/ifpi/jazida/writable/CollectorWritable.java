package br.edu.ifpi.jazida.writable;

import java.io.Serializable;

import org.apache.lucene.search.Collector;

public class CollectorWritable extends AbstractWritable {

	public CollectorWritable() {
	}

	public CollectorWritable(Collector obj) {
		super((Serializable) obj);
	}

}
