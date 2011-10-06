package br.edu.ifpi.jazida.util;

import java.io.IOException;
import java.io.Serializable;
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
import br.edu.ifpi.jazida.node.replication.SupportReplyImage;
import br.edu.ifpi.jazida.node.replication.SupportReplyText;

public class ListsManager {
	private static final Logger LOG = Logger.getLogger(ListsManager.class);
	private static final String HOSTNAME = DataNodeConf.DATANODE_HOSTNAME;
	private static final int REPLY_FREQUENCY = Integer.parseInt(PathJazida.REPLY_FREQUENCY.getValue());
	private static ZooKeeper zk;
	private static PartitionPolicy<NodeStatus> partitionPolicy = new RoundRobinPartitionPolicy();
	private static List<NodeStatus> nodesConnected = new ArrayList<NodeStatus>();
	private static List<String> nodesDesconnected = new ArrayList<String>();
	private static List<NodeStatus> nodesReplySend = new ArrayList<NodeStatus>();
	private static Map<Integer, NodeStatus> mapReplyUtil = new HashMap<Integer, NodeStatus>();
	private static Map<Integer, NodeStatus> cacheMapReplyUtil = new HashMap<Integer, NodeStatus>();
	private static List<NodeStatus> nodesReplyReceive = new ArrayList<NodeStatus>();
	private static List<String> cacheNodesReplyReceive = new ArrayList<String>();
	private static Map<String, List<String>> managerNodesResponding = new HashMap<String, List<String>>();
	private static Map<String, List<String>> historicSendNodesDesconnected = new HashMap<String, List<String>>();
	private static boolean isLoaded = false; 
	private static int idLastNode = 0;
	private static int idFirstNode = 9999999;
	private static int replyFrequency;
	private static int cacheReplyFrequency;
	
	private synchronized static void loadRoundRobinPartitionPolicy(){
		if (!nodesConnected.isEmpty())
			nodesConnected.clear();
		
		nodesConnected = getDatanodesConnected();
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
					
				if(idNode != getIdDatanode(HOSTNAME))
					node = mapReplyUtil.get(idNode);
					
				if(node != null ) 
					nodesReplyReceive.add(node);
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
			int id = getIdDatanode(node.getHostname());
			mapReplyUtil.put(id, node);
			if(id > idLastNode)
				idLastNode = id;
			
			if(id < idFirstNode)
				idFirstNode = id;
		}
	}
	
	private static void loadCache(){
		if(!cacheMapReplyUtil.isEmpty()){
			cacheMapReplyUtil.clear();
		}
		cacheMapReplyUtil.putAll(mapReplyUtil);
		
		cacheReplyFrequency = replyFrequency;
		
		if(!cacheNodesReplyReceive.isEmpty()){ 
			cacheNodesReplyReceive.clear();
		}		
		for(NodeStatus node: nodesReplyReceive){
			cacheNodesReplyReceive.add(node.getHostname());
		}		
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
			String path;
			NodeStatus leader = getDatanodeLeader();
			
			if(!nodesDesconnected.contains(hostNameDesc)){
				nodesDesconnected.add(hostNameDesc);					
			}
			
			if(HOSTNAME.equals(leader.getHostname())){
				zk.setData(ZkConf.DATANODES_DESCONNECTED, Serializer.fromObject((Serializable) nodesDesconnected), -1);
			}			
			
			List<String> listSend = getListNodeSendReply(hostNameDesc);
			if(!historicSendNodesDesconnected.containsKey(hostNameDesc))
				historicSendNodesDesconnected.put(hostNameDesc, listSend);
			
			if(HOSTNAME.equals(leader.getHostname())){
				zk.setData(ZkConf.HISTORIC_SEND, Serializer.fromObject((Serializable) historicSendNodesDesconnected), -1);
			}
			
			if(cacheNodesReplyReceive.contains(hostNameDesc)){
				if(!managerNodesResponding.containsKey(hostNameDesc)){
					List<NodeStatus> nodesExists = null;
					NodeStatus nodeResponder = null;
					
					nodesExists = getDatanodesExists(listSend);
					nodeResponder = getDatanodeReponder(nodesExists, hostNameDesc);
					
					if(nodeResponder != null){
						if(nodeResponder.getHostname().equals(HOSTNAME)){
							path = ZkConf.DATANODES_PATH + "/" + HOSTNAME;
							nodeLocal.getNodesResponding().add(hostNameDesc);
							System.out.println("nodeLocal respondendo: " + nodeLocal.getNodesResponding());					
							try {
								zk.setData(path, Serializer.fromObject(nodeLocal), -1);
							} catch (KeeperException e) {}
						}
					}
					
				} else {
					
					List<NodeStatus> nodesExists = null;
					NodeStatus nodeResponder = null;
					
					nodesExists = getDatanodesExists(listSend);
					nodeResponder = getDatanodeReponder(nodesExists, hostNameDesc);
					
					if(nodeResponder != null){
						if(nodeResponder.getHostname().equals(HOSTNAME)){
							path = ZkConf.DATANODES_PATH + "/" + HOSTNAME;
							nodeLocal.getNodesResponding().add(hostNameDesc);
							System.out.println("nodeLocal respondendo: " + nodeLocal.getNodesResponding());					
							try {
								zk.setData(path, Serializer.fromObject(nodeLocal), -1);
							} catch (KeeperException e) {}
						}
					}
					
					Thread.sleep(3500);
					nodesExists.clear();					
					
					List<String> listNodesResponding = managerNodesResponding.get(hostNameDesc);
					List<String> listHistoric = new ArrayList<String>();
					
					for(String nodeName: listNodesResponding){
						listHistoric = historicSendNodesDesconnected.get(nodeName);
						
						nodesExists = getDatanodesExists(listHistoric);
						nodeResponder = getDatanodeReponder(nodesExists, nodeName);
						
						if((nodeResponder != null) && (!nodeResponder.getHostname().equals(""))){
							path = ZkConf.DATANODES_PATH + "/" + nodeResponder.getHostname();
							nodeResponder.getNodesResponding().add(nodeName);
							try {
								zk.setData(path, Serializer.fromObject(nodeResponder), -1);	
							} catch (KeeperException e) {}
						}
						
						System.out.println(nodeResponder + " = nodeResponder respondendo: " + nodeResponder.getNodesResponding());
					}
				}
				
				if(managerNodesResponding.containsKey(hostNameDesc)){
					managerNodesResponding.remove(hostNameDesc);
				}
				
				if(HOSTNAME.equals(leader.getHostname())){
					zk.setData(ZkConf.MANAGER_NODES_RESPONDING, Serializer.fromObject((Serializable) managerNodesResponding), -1);
				}
				
			}
			
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} catch (KeeperException e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	public synchronized static void managerNodesChanged(String path, NodeStatus nodeLocal) {
		try{
			byte[] bytes = zk.getData(path,	true, null);
			List<String> nodesResponding = new ArrayList<String>();
			NodeStatus datanode = (NodeStatus) Serializer.toObject(bytes);
			
			managerNodesResponding.put(datanode.getHostname(), datanode.getNodesResponding());
			nodesResponding = managerNodesResponding.get(datanode.getHostname());
			if(nodesResponding.isEmpty())
				managerNodesResponding.remove(datanode.getHostname());
			
			if (datanode.getHostname().equals(HOSTNAME)){
				nodeLocal.setNodesResponding(datanode.getNodesResponding());
				zk.setData(ZkConf.MANAGER_NODES_RESPONDING, Serializer.fromObject((Serializable) managerNodesResponding), -1);
			}
						
			System.out.println("managerNodesResponding: " + managerNodesResponding);
			System.out.println("respondendo: " + nodeLocal.getNodesResponding());
			
		} catch (KeeperException e) {
			LOG.error(e.getMessage(), e);
		}catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} 
	}
	
	public synchronized static void managerNodesConnected(String hostName, NodeStatus node) {
		try{
			String path = ZkConf.DATANODES_PATH + "/" + HOSTNAME;
			
			if(!isLoaded){
				loadDada();
			}
			System.out.println("getDatanodesDisconnected(): " + getDatanodesDisconnected());
			System.out.println("historicSendNodesDesconnected " + historicSendNodesDesconnected);
			if(nodesDesconnected.contains(hostName)){
				if (historicSendNodesDesconnected.containsKey(hostName))
					historicSendNodesDesconnected.remove(hostName);
				
				if(node.getNodesResponding().contains(hostName)){
					node.getNodesResponding().remove(node.getNodesResponding().indexOf(hostName));
					zk.setData(path, Serializer.fromObject(node), -1);					
				}
				
				if(hostName.equals(HOSTNAME)){
					nodesDesconnected.remove(hostName);
					zk.setData(ZkConf.DATANODES_DESCONNECTED, Serializer.fromObject((Serializable) nodesDesconnected), -1);					
					zk.setData(ZkConf.HISTORIC_SEND, Serializer.fromObject((Serializable) historicSendNodesDesconnected), -1);
					System.out.println("nodesDesconnected: " +nodesDesconnected);
				}				
				
				
				Thread.sleep(2000);
				if(hostName.equals(HOSTNAME)){
					new SupportReplyText().checkRepliesText(nodesReplyReceive);
					new SupportReplyImage().checkRepliesImage(nodesReplyReceive);
				}				
			} 	
			
			System.out.println("historicSendNodesDesconnected " + historicSendNodesDesconnected);
			System.out.println("managerNodesResponding:  " + managerNodesResponding);
			System.out.println("nodesDesconnected: " + getDatanodesDisconnected());
			System.out.println("nodeLocal respondendo: " + node.getNodesResponding());
			
		
		} catch (KeeperException e) {
			LOG.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
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
	public static List<NodeStatus> getDatanodesConnected() {
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
			LOG.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		
		return datanodes;
	}
	
	private static List<String> getDatanodesDisconnected() {
		List<String> nodes = new ArrayList<String>();
		
		try {
			byte[] bytes = zk.getData(ZkConf.DATANODES_DESCONNECTED, false, null);
			if (bytes != null)
				nodes.addAll((List<String>) Serializer.toObject(bytes));
		} catch (KeeperException e) {
			LOG.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} 
	
		return nodes;
	}
	
	private static NodeStatus getDatanodeLeader() throws KeeperException, InterruptedException, IOException, ClassNotFoundException{
		String path;
		NodeStatus datanode = null;
		
		for(NodeStatus nodeLeader: nodesConnected) {
			path = ZkConf.DATANODES_PATH + "/" + nodeLeader.getHostname();
			if(zk.exists(path, true) != null){
				byte[] bytes = zk.getData(path,	true, null);
				datanode = (NodeStatus) Serializer.toObject(bytes);	
				break;
			}
		}
		return datanode;
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
					
				if(idNode != getIdDatanode(hostName))
					node = cacheMapReplyUtil.get(idNode);
				
				if(node != null ) 
					nodes.add(node.getHostname());
			}
		}
		return nodes;
	}
	
	private static NodeStatus getDatanodeReponder (List<NodeStatus> datanodes, String hostName){
		NodeStatus responder = null;
		int aux = 999999999;
		for(NodeStatus node: datanodes){
			if(!node.getNodesResponding().contains(hostName)){
				if(node.getNodesResponding().size() < aux){
					responder = node;
				}
				aux = node.getNodesResponding().size();
			} else {
				if(responder != null)
					responder.setHostname("");
				break;
			}
		}
		return responder;
	}

	private static List<NodeStatus> getDatanodesExists(List<String> datanodes){
		List<NodeStatus> datanodesExits = new ArrayList<NodeStatus>();
		
		for (String hostName : datanodes) {
			String path = ZkConf.DATANODES_PATH + "/" + hostName;
			try{
				if(zk.exists(path, true) != null){
					byte[] bytes = zk.getData(path,	true, null);
					NodeStatus datanode = (NodeStatus) Serializer.toObject(bytes);
					datanodesExits.add(datanode);	
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
		
		return datanodesExits;
	}
	
	private static boolean loadDada() {
		try{ 
			
			byte[] bytes =  zk.getData(ZkConf.DATANODES_DESCONNECTED, false, null);
			if(bytes != null) {
				List<String> object = (List<String>) Serializer.toObject(bytes);
				nodesDesconnected.addAll(object);
			}
			
			byte[] bytes2 =  zk.getData(ZkConf.MANAGER_NODES_RESPONDING, false, null);
			if(bytes2 != null) {
				Map<String, List<String>> object = (Map<String, List<String>>) Serializer.toObject(bytes2);
				managerNodesResponding.putAll(object);
			}
			
			byte[] bytes3 =  zk.getData(ZkConf.HISTORIC_SEND, false, null);
			if(bytes3 != null) {
				Map<String, List<String>> object = (Map<String, List<String>>) Serializer.toObject(bytes3);
				historicSendNodesDesconnected.putAll(object);
			}
			
			isLoaded = true;
			
		} catch (KeeperException e) {
			LOG.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		}
		
		return isLoaded;
	}
	
	private static int getIdDatanode(String hostname){
		int underline = hostname.lastIndexOf("_");
		int end = hostname.length();
		
		String identificador = hostname.substring(underline + 1, end);
		return Integer.valueOf(identificador);
	}
	
	public static void setZk(ZooKeeper zk) {
		ListsManager.zk = zk;
	}

}
