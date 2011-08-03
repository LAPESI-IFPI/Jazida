package br.edu.ifpi.jazida.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import br.edu.ifpi.jazida.node.DataNode;

/**
 * Centraliza as configurações de um {@link DataNode}.
 * 
 * @author Aécio Santos
 */
public class DataNodeConf {

	private static final Logger LOG = Logger.getLogger(DataNodeConf.class);
	
	public static int TEXT_INDEXER_SERVER_PORT;
	public static int TEXT_SEARCH_SERVER_PORT;
	
	public static int IMAGE_INDEXER_SERVER_PORT;
	public static int IMAGE_SEARCH_SERVER_PORT;
	
	public static int TEXT_REPLICATION_SERVER_PORT;
	public static int IMAGE_REPLICATION_SERVER_PORT;
	
	public static int TEXT_REPLICATION_SUPPORT_SERVER_PORT;
	public static int IMAGE_REPLICATION_SUPPORT_SERVER_PORT;
	
	public static String DATANODE_HOSTADDRESS;
	public static String DATANODE_HOSTNAME;
	
	static {
		try {
			File arquivo = new File("./conf/jazida.node.properties");
			Properties nodeProperties = new Properties();
			nodeProperties.load(new FileInputStream(arquivo));
			
			TEXT_INDEXER_SERVER_PORT = Integer.parseInt(nodeProperties.getProperty("text.indexer.server.port"));
			TEXT_SEARCH_SERVER_PORT = Integer.parseInt(nodeProperties.getProperty("text.searcher.server.port"));
			IMAGE_INDEXER_SERVER_PORT = Integer.parseInt(nodeProperties.getProperty("image.indexer.server.port"));
			IMAGE_SEARCH_SERVER_PORT = Integer.parseInt(nodeProperties.getProperty("image.searcher.server.port"));
			TEXT_REPLICATION_SERVER_PORT = Integer.parseInt(nodeProperties.getProperty("text.replication.server.port"));
			IMAGE_REPLICATION_SERVER_PORT = Integer.parseInt(nodeProperties.getProperty("image.replication.server.port"));
			TEXT_REPLICATION_SUPPORT_SERVER_PORT = Integer.parseInt(nodeProperties.getProperty("text.replication.support.server.port"));
			IMAGE_REPLICATION_SUPPORT_SERVER_PORT = Integer.parseInt(nodeProperties.getProperty("image.replication.support.server.port"));
			DATANODE_HOSTNAME = nodeProperties.getProperty("datanode.hostname");
			DATANODE_HOSTADDRESS = nodeProperties.getProperty("datanode.hostadress");
			
			
			
		} catch (IOException e) {
			LOG.error("Falha na leitura do arquivo de configurações ./conf/jazida.node.properties");
		}
	}
}
