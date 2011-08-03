package br.edu.ifpi.jazida.node;

import java.io.Serializable;

/**
 * Representa o status de um {@link DataNode}. Guarda informações sobre o estado
 * do mesmo como: hostname e o endereço IP.
 * 
 * @author Aécio Solano Rodrigues Santos
 * 
 */
public class NodeStatus implements Serializable {

	private static final long serialVersionUID = 0;

	private String hostname;
	private String address; // Endereço IP do host
	private boolean twoResponding;
	private String hostNameResponding;
	
	private int textSearchServerPort;
	private int textIndexerServerPort;

	private int imageIndexerServerPort;
	private int imageSeacherServerPort;
	
	private int textReplicationServerPort;
	private int imageReplicationServerPort;
	
	private int textReplicationSupportServerPort;
	private int imageReplicationSupportServerPort;
		
	public NodeStatus(String hostname, String address, int textIndexerServerPort, int textSearchServerPort, int imageIndexerServerPort, int imageSearcherServerPort, int textReplicationServerPort, int imageReplicationServerPort, int textReplicationSupportServerPort, int imageReplicationSupportServerPort) {
		this.hostname = hostname;
		this.address = address;
		twoResponding = false;
		hostNameResponding = "";
		this.textIndexerServerPort = textIndexerServerPort;
		this.textSearchServerPort = textSearchServerPort;
		this.imageIndexerServerPort = imageIndexerServerPort;
		this.imageSeacherServerPort = imageSearcherServerPort;
		this.textReplicationServerPort = textReplicationServerPort;
		this.imageReplicationServerPort = imageReplicationServerPort;
		this.textReplicationSupportServerPort = textReplicationSupportServerPort;
		this.imageReplicationSupportServerPort = imageReplicationSupportServerPort;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
	
	public boolean isTwoResponding() {
		return twoResponding;
	}

	public void setTwoResponding(boolean twoResponding) {
		this.twoResponding = twoResponding;
	}
	
	public String getHostNameResponding() {
		return hostNameResponding;
	}

	public void setHostNameResponding(String hostNameResponding) {
		this.hostNameResponding = hostNameResponding;
	}

	public int getTextSearchServerPort() {
		return textSearchServerPort;
	}

	public void setTextSearchServerPort(int textSearchServerPort) {
		this.textSearchServerPort = textSearchServerPort;
	}

	public int getTextIndexerServerPort() {
		return textIndexerServerPort;
	}

	public void setTextIndexerServerPort(int textIndexerServerPort) {
		this.textIndexerServerPort = textIndexerServerPort;
	}

	/**
	 * @param imageIndexerServerPort the imageIndexerServerPort to set
	 */
	public void setImageIndexerServerPort(int imageIndexerServerPort) {
		this.imageIndexerServerPort = imageIndexerServerPort;
	}

	/**
	 * @return the imageIndexerServerPort
	 */
	public int getImageIndexerServerPort() {
		return imageIndexerServerPort;
	}
	
	/**
	 * @param imageSeacherServerPort the imageSeacherServerPort to set
	 */
	public void setImageSearcherServerPort(int imageSeacherServerPort) {
		this.imageSeacherServerPort = imageSeacherServerPort;
	}
	
	/**
	 * @return the imageSeacherServerPort
	 */
	public int getImageSearcherServerPort() {
		return imageSeacherServerPort;
	}
	
	/**
	 * @return the textReplicationServerPort
	 */
	public int getTextReplicationServerPort() {
		return textReplicationServerPort;
	}
	
	/**
	 * @param textReplicationServerPort the textReplicationServerPort to set
	 */
	public void setTextReplicationServerPort(int textReplicationServerPort) {
		this.textReplicationServerPort = textReplicationServerPort;
	}
	
	/**
	 * @return the imageReplicationServerPort
	 */
	public int getImageReplicationServerPort() {
		return imageReplicationServerPort;
	}
	
	/**
	 * @param imageReplicationServerPort the imageReplicationServerPort to set
	 */
	public void setImageReplicationServerPort(int imageReplicationServerPort) {
		this.imageReplicationServerPort = imageReplicationServerPort;
	}
	
	/**
	 * @return the textReplicationSupportServerPort
	 */
	public int getTextReplicationSupportServerPort() {
		return textReplicationSupportServerPort;
	}
	
	/**
	 * @param textReplicationSupportServerPort the textReplicationSupportServerPort to set
	 */
	public void setTextReplicationSupportServerPort(int textReplicationSupportServerPort) {
		this.textReplicationSupportServerPort = textReplicationSupportServerPort;
	}
	
	/**
	 * @return the imageReplicationSupportServerPort
	 */
	public int getImageReplicationSupportServerPort() {
		return imageReplicationSupportServerPort;
	}
	
	/**
	 * @param imageReplicationSupportServerPort the imageReplicationSupportServerPort to set
	 */
	public void setImageReplicationSupportServerPort(int imageReplicationSupportServerPort) {
		this.imageReplicationSupportServerPort = imageReplicationSupportServerPort;
	}
	
	
	@Override
	public String toString() {
		return hostname + "/" + address;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((address == null) ? 0 : address.hashCode());
		result = prime * result
				+ ((hostname == null) ? 0 : hostname.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NodeStatus other = (NodeStatus) obj;
		if (address == null) {
			if (other.address != null)
				return false;
		} else if (!address.equals(other.address))
			return false;
		if (hostname == null) {
			if (other.hostname != null)
				return false;
		} else if (!hostname.equals(other.hostname))
			return false;
		return true;
	}
	
	
}
