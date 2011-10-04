package br.edu.ifpi.jazida.util;

public enum ReturneMessageJazida {
	REPLY_UPDATED("Replica atualizada",102),
	REPLY_OUTDATED("Replica destualizada.",506);

	public final String message;
	public final int code;

	/**
	 * Construtor da enum
	 * @param message - Mensagem de retorno
	 * @param codigo - Código da mensagem
	 */
	ReturneMessageJazida(String message, int codigo) {
		this.message = message;		
		this.code = codigo;
	}

	/**
	 * Retorna a mensagem de retorno da Enum
	 * @return mensagem da enum
	 */
	public String getMessage() {
		return message;
	}
	
	/**
	 * Retorna o código de retorno da Enum
	 * @return código do ReturnMessage
	 */
	public int getCode() {
		return code;
	}
	/**
	 * Devolve a Enum equivalente ao código recebido.
	 * 
	 * @param code
	 * @return ReturnMessage
	 */
	public static ReturneMessageJazida getReturnMessage(int code) {
		ReturneMessageJazida[] enums = ReturneMessageJazida.values();
		for (int i = 0; i < enums.length; i++) {
			if(enums[i].getCode() == code) {
				return enums[i];
			}
		}
		return null;
	}
}
