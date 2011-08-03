package br.edu.ifpi.jazida.exception;

public class NoNodesAvailableException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public NoNodesAvailableException(String message) {
		super(message);
	}
}
