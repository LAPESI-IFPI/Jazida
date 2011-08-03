package br.edu.ifpi.jazida.util;


public enum PropertyJazida {

	OPALA_TEXT_INDEX_REPLY_ADDRESS("opala.text.index.reply.address"),
	OPALA_IMAGE_INDEX_REPLY_ADDRESS("opala.image.index.reply.address"),
	OPALA_REPLY_FREQUENCY("opala.reply.frequency");
	
	String propertyName;

	PropertyJazida(String propertyName) {
		this.propertyName = propertyName;
	}

	public String getPropertyName() {
		return this.propertyName;
	}
}
