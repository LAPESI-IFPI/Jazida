package br.edu.ifpi.jazida.node;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.cluster.ClusterService;
import br.edu.ifpi.jazida.node.protocol.ImageIndexerProtocol;
import br.edu.ifpi.jazida.node.protocol.ImageReplicationProtocol;
import br.edu.ifpi.jazida.node.protocol.ImageSearcherProtocol;
import br.edu.ifpi.jazida.node.protocol.SupportIndexImageProtocol;
import br.edu.ifpi.jazida.node.protocol.SupportIndexTextProtocol;
import br.edu.ifpi.jazida.node.protocol.TextIndexerProtocol;
import br.edu.ifpi.jazida.node.protocol.TextReplicationProtocol;
import br.edu.ifpi.jazida.node.protocol.TextSearchableProtocol;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.opala.indexing.NearRealTimeTextIndexer;
import br.edu.ifpi.opala.utils.IndexManager;
import br.edu.ifpi.opala.utils.Path;

/**
 * Representa um nó conectado ao cluster Jazida. Durante sua inicialização,
 * publica-se no serviço do Zookeeper como disponível e inicializa os servidores
 * de chamada de procedimento remoto (RPC) nas portas especificadas.
 * 
 * @author Aécio Santos
 * 
 */
public class DataNode {

	private static final Logger LOG = Logger.getLogger(DataNode.class);
	private RPCServer textIndexerServer;
	private RPCServer textSearchableServer;
	private RPCServer imageIndexerServer;
	private RPCServer imageSearcherServer;
	private RPCServer textReplicationServer;
	private RPCServer imageReplicationServer;
	private RPCServer textReplicationSupportServer;
	private RPCServer imageReplicationSupportServer;
	
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	private IndexManager indexManager;
	
	/**
	 * Inicia um {@link DataNode} com configurações do host local.
	 * 
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void start() throws UnknownHostException, IOException,
	InterruptedException, KeeperException {
		
		this.start(	DataNodeConf.DATANODE_HOSTNAME, 
					DataNodeConf.DATANODE_HOSTADDRESS,
					DataNodeConf.TEXT_INDEXER_SERVER_PORT,
					DataNodeConf.TEXT_SEARCH_SERVER_PORT,
					DataNodeConf.IMAGE_INDEXER_SERVER_PORT,
					DataNodeConf.IMAGE_SEARCH_SERVER_PORT,
					DataNodeConf.TEXT_REPLICATION_SERVER_PORT,
					DataNodeConf.IMAGE_REPLICATION_SERVER_PORT,
					DataNodeConf.TEXT_REPLICATION_SUPPORT_SERVER_PORT,
					DataNodeConf.IMAGE_REPLICATION_SUPPORT_SERVER_PORT,
					true);
	}

	/**
	 * Inicia um {@link DataNode} com configurações do host local.
	 * 
	 * @param lock
	 *   Se a execução deve ser bloqueada após a inicialização do {@link DataNode}.
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void start(boolean lock) throws UnknownHostException, IOException,
											InterruptedException, KeeperException {

		this.start(	DataNodeConf.DATANODE_HOSTNAME, 
					DataNodeConf.DATANODE_HOSTADDRESS,
					DataNodeConf.TEXT_INDEXER_SERVER_PORT,
					DataNodeConf.TEXT_SEARCH_SERVER_PORT,
					DataNodeConf.IMAGE_INDEXER_SERVER_PORT,
					DataNodeConf.IMAGE_SEARCH_SERVER_PORT,
					DataNodeConf.TEXT_REPLICATION_SERVER_PORT,
					DataNodeConf.IMAGE_REPLICATION_SERVER_PORT,
					DataNodeConf.TEXT_REPLICATION_SUPPORT_SERVER_PORT,
					DataNodeConf.IMAGE_REPLICATION_SUPPORT_SERVER_PORT,
					lock);
	}

	/**
	 * Inicia um {@link DataNode} de acordo com os parâmetros recebidos.
	 * 
	 * @param hostName
	 *            O nome do host em que o DataNode está sendo iniciado.
	 * @param hostAddress
	 *            O endereço IP do host.
	 * @param textIndexerServerPort
	 *            O número da porta que o servidor escutará requisições.
	 * @param textSearchServerPort 
	 * @param lock
	 *            Se a execução será bloqueada após o inicialização do
	 *            {@link DataNode}
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void start(String hostName, String hostAddress,
						int textIndexerServerPort,
						int textSearchServerPort,
						int imageIndexerServerPort,
						int imageSearchServerPort,
						int textReplicationServerPort,
						int imageReplicationServerPort,
						int textReplicationSupportServerPort,
						int imageReplicationSupportServerPort,
						boolean lock) 
	throws IOException, InterruptedException, KeeperException {

		NodeStatus node = new NodeStatus(hostName, hostAddress, 
										textIndexerServerPort,
										textSearchServerPort,
										imageIndexerServerPort,
										imageSearchServerPort,
										textReplicationServerPort,
										imageReplicationServerPort,
										textReplicationSupportServerPort,
										imageReplicationSupportServerPort);
		
		LOG.info("Iniciando o protocolo de RPC ImageIndexerServer");
		File imageIndexPath = new File(Path.IMAGE_INDEX.getValue());
		File imageBackupPath = new File(Path.IMAGE_BACKUP.getValue());
		createIndexIfNotExists(imageIndexPath);
		createIndexIfNotExists(imageBackupPath);
		
		imageIndexerServer = new RPCServer(new ImageIndexerProtocol(),
													node.getAddress(),
													node.getImageIndexerServerPort());
		imageIndexerServer.start(false);

		LOG.info("Iniciando o protocolo de RPC ImageSearchServer");
		imageSearcherServer = new RPCServer(new ImageSearcherProtocol(new SimpleFSDirectory(imageIndexPath), node),
											node.getAddress(),
											node.getImageSearcherServerPort());
		imageSearcherServer.start(false);
		

		FSDirectory directory = FSDirectory.open(new File(Path.TEXT_INDEX.getValue()));
		indexManager = new IndexManager(directory);
		
		LOG.info("Iniciando o protocolo de RPC TextIndexerServer");
		textIndexerServer = new RPCServer(	new TextIndexerProtocol(new NearRealTimeTextIndexer(indexManager)),
											node.getAddress(),
											node.getTextIndexerServerPort());
		textIndexerServer.start(false);

		LOG.info("Iniciando o protocolo de RPC TextSearchableServer");
		textSearchableServer = new RPCServer(new TextSearchableProtocol(indexManager, node),
												node.getAddress(),
												node.getTextSearchServerPort());
		textSearchableServer.start(false);
		
		LOG.info("Iniciando o protocolo de RPC TextReplicationServer");
		textReplicationServer = new RPCServer( new TextReplicationProtocol(),
												node.getAddress(),
												node.getTextReplicationServerPort());
		
		textReplicationServer.start(false);
		
		
		LOG.info("Iniciando o protocolo de RPC ImageReplicationServer");
		imageReplicationServer = new RPCServer( new ImageReplicationProtocol(),
												node.getAddress(),
												node.getImageReplicationServerPort());
		
		imageReplicationServer.start(false);
		
		LOG.info("Iniciando o protocolo de RPC TextReplicationSupportServer");
		textReplicationSupportServer = new RPCServer(new SupportIndexTextProtocol(), 
													node.getAddress(), 
													node.getTextReplicationSupportServerPort());
		
		textReplicationSupportServer.start(false);
		
		LOG.info("Iniciando o protocolo de RPC ImageReplicationSupportServer");
		imageReplicationSupportServer = new RPCServer(new SupportIndexImageProtocol(), 
													node.getAddress(), 
													node.getImageReplicationSupportServerPort());
		
		imageReplicationSupportServer.start(false);
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					stop();
				} catch (InterruptedException e) {
					LOG.error(e);
				}
			}
		}));		
		
		
		new ClusterService(node); 
		if(lock) connectedSignal.await();
	}
	
	public void stop() throws InterruptedException {
		connectedSignal.countDown();
		textIndexerServer.stop();
		textSearchableServer.stop();
		imageIndexerServer.stop();
		imageSearcherServer.stop();
		textReplicationServer.stop();
		imageReplicationServer.stop();
		textReplicationSupportServer.stop();
		imageReplicationSupportServer.stop();
		try {
			indexManager.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOG.info("Execução de DataNode finalizada!");
	}
	
	private void createIndexIfNotExists(File indexPath)
	throws CorruptIndexException, LockObtainFailedException, IOException {

		IndexWriter indexWriter = new IndexWriter(	FSDirectory.open(indexPath),
													new BrazilianAnalyzer(Version.LUCENE_30),
													IndexWriter.MaxFieldLength.UNLIMITED);
		indexWriter.close();
	}

	
}
