package br.edu.ifpi.jazida.cluster;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
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
			ListsManager.setZk(zk);
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
			
			} else {
				LOG.fatal("Um datanode com este hostname ja esta conectado ao grupo. Reinicie o datanode com outro 'hostName'.");
				LOG.fatal("Reinicie o datanode com outro 'hostName'.");
				List<String> listDesc = (List<String>) Serializer.toObject(zk.getData(ZkConf.DATANODES_DESCONNECTED, false, null)); 
				List<String> listConected = zk.getChildren(ZkConf.DATANODES_PATH, true);
				Map<String, List<String>> object = (Map<String, List<String>>) Serializer.toObject(zk.getData(ZkConf.MANAGER_NODES_RESPONDING, false, null));
				if(listDesc != null){
					LOG.info("HostName utilizado: " +hostName);
					if (listDesc.size() > 0)
						LOG.info("Datanodes desconectados: " + listDesc);
					if (listConected.size() > 0)
						LOG.info("Datanodes conectados: " + listConected);
				} else {
					LOG.info("Datanodes conectados: " + listConected);
				}
				
				connectedSignal.await();
			}
			
			List<String> listConected = zk.getChildren(ZkConf.DATANODES_PATH, true);
			if (listConected.size() == 1){
				if (zk.exists(ZkConf.DATANODES_DESCONNECTED, false) != null) {
					zk.delete(ZkConf.DATANODES_DESCONNECTED, -1);
				}
				
				if (zk.exists(ZkConf.HISTORIC_SEND, false) != null) {
					zk.delete(ZkConf.HISTORIC_SEND, -1);
				}
				
				if (zk.exists(ZkConf.MANAGER_NODES_RESPONDING, false) != null) {
					zk.delete(ZkConf.MANAGER_NODES_RESPONDING, -1);
				}
			}
			
			if (zk.exists(ZkConf.DATANODES_DESCONNECTED, false) == null) {
				List<String> nodesDesconnected = new ArrayList<String>();
				zk.create(ZkConf.DATANODES_DESCONNECTED, Serializer.fromObject((Serializable) nodesDesconnected), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
			if (zk.exists(ZkConf.HISTORIC_SEND, false) == null) {
				Map<String, List<String>> historicSendNodesDesconnected = new HashMap<String, List<String>>();
				zk.create(ZkConf.HISTORIC_SEND, Serializer.fromObject((Serializable) historicSendNodesDesconnected), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
			if (zk.exists(ZkConf.MANAGER_NODES_RESPONDING, false) == null) {
				Map<String, List<String>> managerNodesResponding = new HashMap<String, List<String>>();
				zk.create(ZkConf.MANAGER_NODES_RESPONDING, Serializer.fromObject((Serializable) managerNodesResponding), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
		} catch (KeeperException e) {
			LOG.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
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
				zk.sync(path, this, null);
				break;
	
			case NodeDeleted:
				zk.sync(path, this, null);
				break;
			
			case NodeDataChanged:
				int begin = path.lastIndexOf("/");
				int end = path.length();
				String hostName = path.substring(begin + 1, end);
				
				ListsManager.managerNodesChanged(hostName, node);
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
			if(!path.contains(ZkConf.DATANODES_DESCONNECTED) ||
					!path.contains(ZkConf.HISTORIC_SEND) ||
						!path.contains(ZkConf.MANAGER_NODES_RESPONDING)){
				
				int begin = path.lastIndexOf("/");
				int end = path.length();
				String hostName = path.substring(begin + 1, end);
				
				if((zk.exists(path, true) == null)) {
					LOG.info("Um datanode desconectou-se do cluster.");
					LOG.info("Datanode que se desconectou: " + hostName);
					ListsManager.managerDatanodesDeleted(hostName, node);
				} else {
					LOG.info("Datanode que se conectou: " + hostName);
					ListsManager.managerDatanodesCreated(hostName, node);
				}
			}
				
		} catch (KeeperException e) {
			LOG.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		}
	}	
	
	public static void disconnect() throws InterruptedException {
		LOG.info("Desconectando-se ao Zookeeper...");
		zk.close();
		LOG.info("Desconectado.");
	}

}
