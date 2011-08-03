package br.edu.ifpi.jazida.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Centraliza as configurações do Zookeeper utilizadas no Jazida. Servidores do
 * Zookeeper devem ser configurados no arquivo ${JAZIDA_HOME}/conf/jazida.zk.properties.
 * 
 * @author Aécio Solano Rodrigues Santos
 * 
 */
public class ZkConf {

	private static final Logger LOG = Logger.getLogger(ZkConf.class);
	
	public static String ZOOKEEPER_SERVERS;
	public static int ZOOKEEPER_TIMEOUT;
	public static int ZOOKEEPER_TICKTIME;
	public static String DATANODES_PATH;
	public static String HISTORIC_PATH;

	static {
		try {
			File arquivo = new File("./conf/jazida.zk.properties");
			Properties properties = new Properties();
			properties.load(new FileInputStream(arquivo));
			ZOOKEEPER_SERVERS = properties.getProperty("zookeeper.servers");
			ZOOKEEPER_TIMEOUT = Integer.parseInt(properties.getProperty("zookeeper.timeout"));
			ZOOKEEPER_TICKTIME = Integer.parseInt(properties.getProperty("zookeeper.ticktime"));
			DATANODES_PATH = properties.getProperty("zookeeper.path.datanodes");
			HISTORIC_PATH = properties.getProperty("zookeeper.path.historic");
		} catch (IOException e) {
			LOG.error("Falha na leitura do arquivo de configurações ./conf/jazida.zk.properties");
		}
	}

}