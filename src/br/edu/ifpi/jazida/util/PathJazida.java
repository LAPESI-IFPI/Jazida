package br.edu.ifpi.jazida.util;


public enum PathJazida {
	
	TEXT_INDEX_REPLY	(ConfigurationJazida.getInstance().getPropertyValue(PropertyJazida.OPALA_TEXT_INDEX_REPLY_ADDRESS)),
	IMAGE_INDEX_REPLY	(ConfigurationJazida.getInstance().getPropertyValue(PropertyJazida.OPALA_IMAGE_INDEX_REPLY_ADDRESS)),
	REPLY_FREQUENCY		(ConfigurationJazida.getInstance().getPropertyValue(PropertyJazida.OPALA_REPLY_FREQUENCY));
	
	private final String value;
	
	/**
	 * Construtor da enum
	 * @param value - caminho do diretório
	 */
	PathJazida(String value) {
		this.value = value;
	}
	
	/**
	 * Retorna o caminho do Path
	 * @return caminho do Path
	 */
	public String getValue(){
		return this.value;
	}
	
	/**
	 * O mesmo que getValue()
	 */
	public String toString(){
		return getValue();
	}

}
