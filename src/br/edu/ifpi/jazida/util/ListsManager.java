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
	private static List<NodeStatus> nodesReplySend = new ArrayList<NodeStatus>();
	private static Map<Integer, NodeStatus> mapReplyUtil = new HashMap<Integer, NodeStatus>();
	private static Map<Integer, NodeStatus> cacheMapReplyUtil = new HashMap<Integer, NodeStatus>();
	private static List<String> nodesReplyReceive = new ArrayList<String>();
	private static List<String> cacheNodesReplyReceive = new ArrayList<String>();
	private static Map<String, List<String>> managerNodesResponding = new HashMap<String, List<String>>();
	private static Map<String, List<String>> historicSendNodesDesconnected = new HashMap<String, List<String>>();
	private static int idLastNode = 0;
	private static int idFirstNode = 9999999;
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
					
				if(idNode != getIdDatanode(HOSTNAME))
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
			String path;

			nodesDesconnected.add(hostNameDesc);
			List<String> listSend = getListNodeSendReply(hostNameDesc);
			historicSendNodesDesconnected.put(hostNameDesc, listSend);
			
			if(cacheNodesReplyReceive.contains(hostNameDesc)){
				if(!managerNodesResponding.containsKey(hostNameDesc)){
					List<NodeStatus> nodesExists = new ArrayList<NodeStatus>();
					NodeStatus nodeResponder = null;
					int aux = 999999999;
					
					for (String hostName : listSend) {
						path = ZkConf.DATANODES_PATH + "/" + hostName;
						try{
							if(zk.exists(path, true) != null){
								byte[] bytes = zk.getData(path,	true, null);
								NodeStatus datanode = (NodeStatus) Serializer.toObject(bytes);
								nodesExists.add(datanode);	
							}
						} catch (KeeperException e) {}
					}
				
					for(NodeStatus node: nodesExists){
						if(!node.getNodesResponding().contains(hostNameDesc)){
							if(node.getNodesResponding().size() < aux){
								nodeResponder = node;
							}
							aux = node.getNodesResponding().size();
						} else {
							if(nodeResponder != null)
								nodeResponder.setHostname("");
							break;
						}
					}
					
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
					
					List<NodeStatus> nodesExists = new ArrayList<NodeStatus>();
					NodeStatus nodeResponder = null;
					int aux = 999999999;
					
					for (String hostName : listSend) {
						path = ZkConf.DATANODES_PATH + "/" + hostName;
						try{
							byte[] bytes = zk.getData(path,	true, null);
							NodeStatus datanode = (NodeStatus) Serializer.toObject(bytes);
							nodesExists.add(datanode);
						} catch (KeeperException e) {}
					}
					
					for(NodeStatus node: nodesExists){
						if(!node.getNodesResponding().contains(hostNameDesc)){
							if(node.getNodesResponding().size() < aux){
								nodeResponder = node;
							}
							aux = node.getNodesResponding().size();
						} else {
							if(nodeResponder != null)
								nodeResponder.setHostname("");
							break;
						}
					}
					
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
					
					Thread.sleep(1500);
					nodesExists.clear();					
					
					List<String> listNodesResponding = managerNodesResponding.get(hostNameDesc);
					List<String> listHistoric = new ArrayList<String>();
					aux = 999999999;
					for(String nodeName: listNodesResponding){
						listHistoric = historicSendNodesDesconnected.get(nodeName);
						for (String hostName : listHistoric) {
							path = ZkConf.DATANODES_PATH + "/" + hostName;
							try{
								if(zk.exists(path, true) != null){
									byte[] bytes = zk.getData(path,	true, null);
									NodeStatus datanode = (NodeStatus) Serializer.toObject(bytes);
									nodesExists.add(datanode);	
								}
							} catch (KeeperException e) {}
						}
						
						for(NodeStatus node: nodesExists){
							if(!node.getNodesResponding().contains(nodeName)){
								if(node.getNodesResponding().size() < aux){
									nodeResponder = node;
								}
								aux = node.getNodesResponding().size();
							} else {
								if(nodeResponder != null)
									nodeResponder.setHostname("");
								break;
							}
						}
						
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
				
			}
			
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		} catch (ClassNotFoundException e) {
			LOG.error(e);
		}
	}
	
	public synchronized static void managerNodesChanged(String path, NodeStatus node) {
		try{
			byte[] bytes = zk.getData(path,	true, null);
			List<String> nodesResponding = new ArrayList<String>();
			NodeStatus datanode = (NodeStatus) Serializer.toObject(bytes);
			if (datanode.getHostname().equals(HOSTNAME))
				node.setNodesResponding(datanode.getNodesResponding());
			
			managerNodesResponding.put(datanode.getHostname(), datanode.getNodesResponding());
			nodesResponding = managerNodesResponding.get(datanode.getHostname());
			if(nodesResponding.isEmpty())
				managerNodesResponding.remove(datanode.getHostname());
			
			System.out.println("managerNodesResponding: " + managerNodesResponding);
			System.out.println("respondendo: " + node.getNodesResponding());
			
		} catch (KeeperException e) {
			LOG.error(e);
		}catch (InterruptedException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		} catch (ClassNotFoundException e) {
			LOG.error(e);
		} 
	}
	
	public synchronized static void managerNodesConnected(String hostName, NodeStatus node) {
		try{
			String path = ZkConf.DATANODES_PATH + "/" + HOSTNAME;
			
			if(nodesDesconnected.contains(hostName)){
				if (historicSendNodesDesconnected.containsKey(hostName))
					historicSendNodesDesconnected.remove(hostName);
				
				if(node.getNodesResponding().contains(hostName)){
					nodesDesconnected.remove(nodesDesconnected.indexOf(hostName));
					node.getNodesResponding().remove(node.getNodesResponding().indexOf(hostName));
					System.out.println("node resp.: " + node.getNodesResponding());
					zk.setData(path, Serializer.fromObject(node), -1);
					
				} else{
					nodesDesconnected.remove(nodesDesconnected.indexOf(hostName));
				}
				
				System.out.println("nodesDesconnected: " + nodesDesconnected);
				System.out.println("nodeLocal respondendo: " + node.getNodesResponding());			
				
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
					
				if(idNode != getIdDatanode(hostName))
					node = cacheMapReplyUtil.get(idNode);
				
				if(node != null ) 
					nodes.add(node.getHostname());
			}
		}
		return nodes;
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

//	private static NodeStatus getNextNode(String path){

//			NodeStatus node = null;
//			try{
//				byte[] bytes = zk.getData(path,	true, null);
//				node = (NodeStatus) Serializer.toObject(bytes);			
//			} catch (ClassNotFoundException e) {
//				LOG.error(e);
//			}  catch (KeeperException e) {
//				
//			} catch (InterruptedException e) {
//				LOG.error(e);
//			} catch (IOException e) {
//				LOG.error(e);
//			}
//			
//			return node;
//			
//		}

}
