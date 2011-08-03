package br.edu.ifpi.jazida.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.util.ListsManager;
import br.edu.ifpi.jazida.util.Serializer;
import br.edu.ifpi.jazida.util.ZkConf;

/**
 * Realiza a conexão do Jazida com o serviço do Zookeeper.
 * 
 * @author Aécio Solano Rodrigues Santos
 * 
 */
public class ClusterService implements Watcher, VoidCallback {

	private static final Logger LOG = Logger.getLogger(ClusterService.class);
	public static ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	private NodeStatus node;
	
	public ClusterService(NodeStatus node)
			throws KeeperException, InterruptedException, IOException {
		this.node = node;
		startZookeeper();	
	}

	private void startZookeeper() {
		try {
			zk = new ZooKeeper(ZkConf.ZOOKEEPER_SERVERS, ZkConf.ZOOKEEPER_TIMEOUT, this);
			connectedSignal.await();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}

	private void registerOnClusterService(String hostName, NodeStatus node) {
		try {
			if (zk.exists(ZkConf.DATANODES_PATH, true) == null) {
				zk.create(ZkConf.DATANODES_PATH, null, Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}
			
			LOG.info("Conectando-se ao Cluster Service...");
				
			String path = ZkConf.DATANODES_PATH + "/" + hostName;
			if((zk.exists(path, true) == null)) {
				zk.create(path, Serializer.fromObject(node), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				LOG.info("Conectado ao grupo.");
			}
			else {
				LOG.fatal("Um datanode com este hostname ja esta conectado ao grupo. Utilize outro 'hostname'.");
			}
			
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		}
	}
	
	private void registerHistoricClusterService(String hostName) {
		try {
			
			if (zk.exists(ZkConf.HISTORIC_PATH, false) == null) {
				zk.create(ZkConf.HISTORIC_PATH, null, Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}
			String pathHistoric = ZkConf.HISTORIC_PATH + "/" + hostName;
			if((zk.exists(pathHistoric, false) == null)) {
				zk.create(pathHistoric, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		}
	}
	

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		
		if (event.getType() == Event.EventType.None) {
			switch (event.getState()) {
			case SyncConnected:
				registerOnClusterService(node.getHostname(), node);
				ListsManager.manager();
				registerHistoricClusterService(node.getHostname());
				ListsManager.loadMemoryHistoricNodes();
			// passou para o manage	ListsManager.loadNodesReceiveReply();
				// talvez nao precise  ListsManager.loadNodesDisconneted();
				connectedSignal.countDown();				
				break;

			case Expired:
				startZookeeper();
				break;

			case Disconnected:
				LOG.fatal("Esse datanode desconectou-se do cluster, conecte-o novamente a rede.");
				break;
			}

		} else {		
			switch (event.getType()) {
			case NodeCreated:
				LOG.info("Um datanode conectou-se do cluster.");
				zk.sync(path, this, null);
				break;
	
			case NodeDeleted:
				LOG.info("Um datanode desconectou-se do cluster.");
				zk.sync(path, this, null);
				break;
			
			case NodeDataChanged:
				ListsManager.managerNodesChanged(path, zk);
				break;
				
			case NodeChildrenChanged:
				ListsManager.manager();
				break;
			}		
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx) {
		try {
			Map<String, String> historic = ListsManager.getHistoric();
			
			if(path.equals(ZkConf.HISTORIC_PATH)){
				ListsManager.loadMemoryHistoricNodes();			
			} else {
				String hostName = historic.get(path);
				if((zk.exists(path, true) == null)) {
					LOG.info("Datanode que se desconectou: " + hostName);
					ListsManager.managerNodesDeleted(hostName, node, zk);
				} else {
					LOG.info("Datanode que se conectou: " + hostName);
					ListsManager.managerNodesConnected(hostName, node, zk);
				}
			}
				
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		}
	}
	
	/**
	 * Lista os {@link DataNode}s conectados no momento ao ClusterService.
	 * 
	 * @return datanodes
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static List<NodeStatus> getDataNodes() {
		List<NodeStatus> datanodes = new ArrayList<NodeStatus>();
		try {
			List<String> nodesIds = zk.getChildren(ZkConf.DATANODES_PATH, true);
			LOG.info("Cluster com " + nodesIds.size() + " datanode(s) ativo(s) no momento.");
	
			for (String hostName : nodesIds) {				
					String path = ZkConf.DATANODES_PATH + "/" + hostName;
					byte[] bytes = zk.getData(path,	true, null);
					NodeStatus node = (NodeStatus) Serializer.toObject(bytes);
					datanodes.add(node);
			}
		
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		}  catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		}
		
		return datanodes;
	}
	
	public static List<String> getHistoricDataNodes() {
		List<String> hostNames = new ArrayList<String>();
		
		try {
			List<String> historicIds = zk.getChildren(ZkConf.HISTORIC_PATH, false);
			
			for (String hostName : historicIds) {
				hostNames.add(hostName);
			}
		} catch (KeeperException e) {
			LOG.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		} 
		
		return hostNames;
	}
	
	
	public static void disconnect() throws InterruptedException {
		LOG.info("Desconectando-se ao Zookeeper...");
		zk.close();
		LOG.info("Desconectado.");
	}

}
