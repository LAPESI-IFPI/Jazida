package br.edu.ifpi.jazida.writable;

import org.apache.lucene.search.Weight;

public class WeightWritable extends AbstractWritable {

	public WeightWritable() {
	}

	public WeightWritable(Weight obj) {
		super(obj);
	}
	
	public Weight getWeight() {
		return (Weight) super.getObject();
	}
}
