package br.edu.ifpi.jazida.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import br.edu.ifpi.jazida.client.PartitionPolicy;
import br.edu.ifpi.jazida.client.RoundRobinPartitionPolicy;
import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.jazida.node.NodeStatus;

public class ListsManager {
	private static final Logger LOG = Logger.getLogger(ListsManager.class);
	private static final String HOSTNAME = DataNodeConf.DATANODE_HOSTNAME;
	private static final int REPLY_FREQUENCY = Integer.parseInt(PathJazida.REPLY_FREQUENCY.getValue());
	private static ZooKeeper zk;
	private static PartitionPolicy<NodeStatus> partitionPolicy = new RoundRobinPartitionPolicy();
	private static List<NodeStatus> nodesConnected = new ArrayList<NodeStatus>();
	private static List<String> nodesDesconnected = new ArrayList<String>();
//	private static List<String> nodesHistoric = new ArrayList<String>();
//	private static Map<Integer, String> historic = new HashMap<Integer, String>();
	private static List<NodeStatus> nodesReplySend = new ArrayList<NodeStatus>();
	private static Map<Integer, NodeStatus> mapReplyUtil = new HashMap<Integer, NodeStatus>();
	private static Map<Integer, NodeStatus> cacheMapReplyUtil = new HashMap<Integer, NodeStatus>();
	private static List<String> nodesReplyReceive = new ArrayList<String>();
	private static List<String> cacheNodesReplyReceive = new ArrayList<String>();
	//private static Map<Integer, String> mapReplyReceiveUtil = new HashMap<Integer, String>();
	private static Map<String, String> managerNodesResponding = new HashMap<String, String>();
	private static int idLastNode = 0;
	private static int idFirstNode = 1;
	private static int replyFrequency;
	private static int cacheReplyFrequency;
	
	private synchronized static void loadRoundRobinPartitionPolicy(){
		if (!nodesConnected.isEmpty())
			nodesConnected.clear();
		
		nodesConnected = getDataNodesConnected();
		partitionPolicy.clear();
		partitionPolicy.addNodes(nodesConnected.toArray(new NodeStatus[nodesConnected.size()]));
	}	

	private synchronized static void loadNodesReplySend() {
		if(!nodesReplySend.isEmpty())
			nodesReplySend.clear();		
		
		int idNode = getIdDatanode(HOSTNAME);
		for(int i=0; i < replyFrequency; i++){
			NodeStatus node = null;
			if(nodesConnected.size() > 1){
				while(node == null){
					idNode++;
					if(idNode > idLastNode)
						idNode = idFirstNode;
					
					node = mapReplyUtil.get(idNode);
					
					if(node != null ) 
						nodesReplySend.add(node);
				}
			}
		}		
	}
	
	private synchronized static void loadNodesReplyReceive() {
		if(!nodesReplyReceive.isEmpty())
			nodesReplyReceive.clear();
	
		int idNode = getIdDatanode(HOSTNAME);
		for(int i=0; i < replyFrequency; i++){
			NodeStatus node = null;
			while(node == null){
				idNode--;
				if(idNode == 0)
					idNode = idLastNode;
					
				node = mapReplyUtil.get(idNode);
					
				if(node != null ) 
					nodesReplyReceive.add(node.getHostname());
			}
		}
	}
	
	private synchronized static void setUpSendReceiveReply(){
		if(nodesConnected.isEmpty())
			loadRoundRobinPartitionPolicy();
		
		if(nodesConnected.size() <= REPLY_FREQUENCY){
			replyFrequency = (nodesConnected.size() - 1);
		} else {
			replyFrequency = REPLY_FREQUENCY;
		}
		
		if(!mapReplyUtil.isEmpty())
			mapReplyUtil.clear();
	
		for(NodeStatus node: nodesConnected){
			if(!node.getHostname().equals(HOSTNAME)){
				int id = getIdDatanode(node.getHostname());			
				mapReplyUtil.put(id, node);
				if(id > idLastNode){
					idLastNode = id;
				}
			}
		}
	}
	
	private static void loadCache(){
		cacheMapReplyUtil.putAll(mapReplyUtil);
		cacheReplyFrequency = replyFrequency;
		if(!cacheNodesReplyReceive.isEmpty()) 
			cacheNodesReplyReceive.clear();
		cacheNodesReplyReceive.addAll(nodesReplyReceive);
	}
	
	public static void manager(){
		loadRoundRobinPartitionPolicy();
		loadCache();
		setUpSendReceiveReply();
		loadNodesReplySend();
		loadNodesReplyReceive();
	}
	
	public synchronized static void managerNodesDeleted(String hostNameDesc, NodeStatus nodeLocal) {
		try{
			int idNodeDesconnected = getIdDatanode(hostNameDesc);
			int idNextNode = idNodeDesconnected + 1;
			int idNodeLocal = getIdDatanode(nodeLocal.getHostname());
			String pathNextNode = ZkConf.DATANODES_PATH + "/host_" + String.valueOf(idNextNode);
			String path;
			
			if(cacheNodesReplyReceive.contains(hostNameDesc)){
				nodesDesconnected.add(hostNameDesc);
				
				if(!nodeLocal.isTwoResponding()){
					NodeStatus nextNode = getNextNode(pathNextNode);
					
					if(idNodeLocal == idNextNode){
						String hostRenponder = hostNameDesc;
						if(managerNodesResponding.containsKey(hostNameDesc)){
							hostRenponder = managerNodesResponding.get(hostNameDesc);
						}
						path = ZkConf.DATANODES_PATH + "/" + nodeLocal.getHostname();
						nodeLocal.setTwoResponding(true);
						nodeLocal.setHostNameResponding(hostRenponder);
						zk.setData(path, Serializer.fromObject(nodeLocal), -1);
					
					} else if (( nextNode == null) && 
							  (!managerNodesResponding.containsKey(hostNameDesc))){
						List<String> listNodes = getListNodeSendReply(hostNameDesc);
						boolean nodeResponding = false;
						NodeStatus nodeResponder = null;
						int lesser;
						int nearest = idLastNode;
						
						for (String hostName : listNodes) {
							path = ZkConf.DATANODES_PATH + "/" + hostName;
							byte[] bytes = zk.getData(path,	true, null);
							if(bytes != null){
								NodeStatus datanode = (NodeStatus) Serializer.toObject(bytes);
								if(datanode.getHostNameResponding().equals(hostNameDesc)){
									nodeResponding = true;
								}
								
								int id = getIdDatanode(datanode.getHostname());
								if(!datanode.isTwoResponding()){
									if (id > idNodeDesconnected){
										lesser = id - idNodeDesconnected;
									} else{
										lesser = idNodeDesconnected - id;
									}
									
									if(lesser < nearest){
										nearest = lesser;
										nodeResponder = datanode;
									}
								}								
							}
						}
						
						if((nodeResponding == false) && 
								(nodeResponder.getHostname().equals(nodeLocal.getHostname()))){
							path = ZkConf.DATANODES_PATH + "/" + nodeLocal.getHostname();
							nodeLocal.setTwoResponding(true);
							nodeLocal.setHostNameResponding(hostNameDesc);
							zk.setData(path, Serializer.fromObject(nodeLocal), -1);
						}
					}
				}
			}
			
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		} catch (ClassNotFoundException e) {
			LOG.error(e);
		}
	}
	
	public synchronized static void managerNodesChanged(String path) {
		try{
			byte[] bytes = zk.getData(path,	true, null);
			if(bytes != null){
				NodeStatus datanode = (NodeStatus) Serializer.toObject(bytes);
				if(managerNodesResponding.containsKey(datanode.getHostname())){
					managerNodesResponding.remove(datanode.getHostname());
				} else{
					managerNodesResponding.put(datanode.getHostname(), datanode.getHostNameResponding());					
				}
			}
			
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		} catch (ClassNotFoundException e) {
			LOG.error(e);
		} 
	}
	
	public synchronized static void managerNodesConnected(String hostName, NodeStatus node) {
		try{
			String path = ZkConf.DATANODES_PATH + node.getHostname();
			
			if(nodesDesconnected.contains(hostName)){
				if(hostName.equals(node.getHostNameResponding())){
					nodesDesconnected.remove(nodesDesconnected.indexOf(hostName));
					node.setTwoResponding(false);
					node.setHostNameResponding("");
					zk.setData(path, Serializer.fromObject(node), -1);
					
				} else{
					nodesDesconnected.remove(nodesDesconnected.indexOf(hostName));
				}
			}
		
		} catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		} 
	}
	
	public static void setZk(ZooKeeper zk) {
		ListsManager.zk = zk;
	}

	public static List<NodeStatus> getDataNodes(){
		if (nodesConnected.isEmpty())
			loadRoundRobinPartitionPolicy();
		
		return nodesConnected;
	}
	
	public static List<NodeStatus> getNodesReplication(){
		if (nodesReplySend.isEmpty()){
			setUpSendReceiveReply();
			loadNodesReplySend();
		}
		
		return nodesReplySend;
	}
	
	public static NodeStatus nextNode() {
		return partitionPolicy.nextNode();
	}
	
	/**
	 * Lista os {@link DataNode}s conectados no momento ao ClusterService.
	 * 
	 * @return datanodes
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static List<NodeStatus> getDataNodesConnected() {
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
	
	private static List<String> getListNodeSendReply(String hostName){
		List<String> nodes = new ArrayList<String>();
		
		int idNode = getIdDatanode(hostName);
		for(int i=0; i < cacheReplyFrequency; i++){
			NodeStatus node = null;
			while(node == null){
				idNode++;
				if(idNode > idLastNode)
					idNode = idFirstNode;
					
				node = cacheMapReplyUtil.get(idNode);
				
				if(node != null ) 
					nodes.add(node.getHostname());
			}
		}
		return nodes;
	}

	private static NodeStatus getNextNode(String path){
	
		NodeStatus node = null;
		try{
			byte[] bytes = zk.getData(path,	true, null);
			if(bytes != null)
				node = (NodeStatus) Serializer.toObject(bytes);
			
		} catch (ClassNotFoundException e) {
			LOG.error(e);
		}  catch (KeeperException e) {
			LOG.error(e);
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		}
		
		return node;
		
	}

	private static int getIdDatanode(String hostname){
		int underline = hostname.lastIndexOf("_");
		int end = hostname.length();
		
		String identificador = hostname.substring(underline + 1, end);
		int id = Integer.valueOf(identificador);
		return id;
	}
//	public static void loadNodesDisconneted() {
//		boolean exists = false;
//		for(String hostName: nodesReplyReceive){
//			for(NodeStatus node: nodesConnected){
//				if(hostName.equals(node.getHostname())){
//					exists = true;
//					break;
//				}
//			}
//			
//			if (exists == false) {
//				nodesDesconnected.add(hostName);
//			}
//		
//			exists = false;
//		}
//	}

//	public static void loadMemoryHistoricNodes() {
//	nodesHistoric = ClusterService.getHistoricDataNodes();
//	for(String hostName: nodesHistoric){
//		int id = getIdDatanode(hostName);
//		if(!historic.containsKey(id))
//			historic.put(id, hostName);
//	}
//}


}
