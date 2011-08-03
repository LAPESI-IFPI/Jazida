package br.edu.ifpi.jazida.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import br.edu.ifpi.jazida.client.PartitionPolicy;
import br.edu.ifpi.jazida.client.RoundRobinPartitionPolicy;
import br.edu.ifpi.jazida.cluster.ClusterService;
import br.edu.ifpi.jazida.node.NodeStatus;

public class ListsManager {
	private static final String HOSTNAME = DataNodeConf.DATANODE_HOSTNAME;
	private static final int REPLY_FREQUENCY = Integer.parseInt(PathJazida.REPLY_FREQUENCY.getValue());
	private static PartitionPolicy<NodeStatus> partitionPolicy = new RoundRobinPartitionPolicy();
	private static List<NodeStatus> nodesConnected = new ArrayList<NodeStatus>();
	private static List<String> nodesDesconnected = new ArrayList<String>();
	private static List<String> nodesHistoric = new ArrayList<String>();
	private static Map<String, String> historic = new HashMap<String, String>();
	private static List<NodeStatus> nodesReplySend = new ArrayList<NodeStatus>();
	private static Map<Integer, NodeStatus> mapReplyUtil = new HashMap<Integer, NodeStatus>();
	private static List<String> nodesReplyReceive = new ArrayList<String>();
	//private static Map<Integer, String> mapReplyReceiveUtil = new HashMap<Integer, String>();
	private static Map<String, String> managerNodesResponding = new HashMap<String, String>();
	private static int idLastNode = 0;
	private static int replyFrequency;
	
	public synchronized static void loadRoundRobinPartitionPolicy(){
		if (!nodesConnected.isEmpty())
			nodesConnected.clear();
		
		nodesConnected = ClusterService.getDataNodes();
		partitionPolicy.clear();
		partitionPolicy.addNodes(nodesConnected.toArray(new NodeStatus[nodesConnected.size()]));
	}	

	public synchronized static void loadNodesReplySend() {
		int idFirstNode = 01;

		if(nodesConnected.isEmpty())
			loadRoundRobinPartitionPolicy();
		
		if(nodesConnected.size() <= REPLY_FREQUENCY){
			replyFrequency = (nodesConnected.size() - 1);
		} else {
			replyFrequency = REPLY_FREQUENCY;
		}
		
		if(!mapReplyUtil.isEmpty())
			mapReplyUtil.clear();
		
		if(!nodesReplySend.isEmpty())
			nodesReplySend.clear();
		
		for(NodeStatus node: nodesConnected){
			if(!node.getHostname().equals(HOSTNAME)){
				int id = getIdDatanode(node.getHostname());			
				mapReplyUtil.put(id, node);
				if(id > idLastNode){
					idLastNode = id;
				}
			}
		}
		
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
	
	//Analisar, acho que deve ser de quem esta conectado.
	public synchronized static void loadNodesReplyReceive() {
		
//		nodesHistoric = ClusterService.getHistoricDataNodes();
//	
//		if(!nodesReplyReceive.isEmpty())
//			nodesReplyReceive.clear();
//		
//		for(String hostName: nodesHistoric){
//			if(!mapReplyReceiveUtil.containsValue(hostName)){
//				if(!hostName.equals(HOSTNAME)){
//					int id = getIdDatanode(hostName);			
//					mapReplyReceiveUtil.put(id, hostName);
//					if(id > idLastNode){
//						idLastNode = id;
//					}
//				}
//			}
//		}
		
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
	
	public static void manager(){
		loadRoundRobinPartitionPolicy();
		loadNodesReplySend();
		loadNodesReplyReceive();
	}
	
	//coorrendo risco de sair
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
	
	
	public synchronized static void managerNodesDeleted(String hostNameDesc, NodeStatus node, ZooKeeper zooKeeper) {
		try{
			int idNodeDesconnected = getIdDatanode(hostNameDesc);
			int idNextNode = idNodeDesconnected + 1;
			int idNode = getIdDatanode(node.getHostname());
			String path;
			
			if(nodesReplyReceive.contains(hostNameDesc)){
				nodesDesconnected.add(hostNameDesc);
				
				if(!node.isTwoResponding()){
					
					if(idNode == idNextNode){
						String hostRenponder = hostNameDesc;
						if(managerNodesResponding.containsKey(hostNameDesc)){
							hostRenponder = managerNodesResponding.get(hostNameDesc);
						}
						path = ZkConf.DATANODES_PATH + node.getHostname();
						node.setTwoResponding(true);
						node.setHostNameResponding(hostRenponder);
						zooKeeper.setData(path, Serializer.fromObject(node), -1);
					
					} else {
						List<NodeStatus> listNodes = getListNodeSendReply(hostNameDesc);
						List<NodeStatus> nodes = new ArrayList<NodeStatus>();
						boolean nodeResponding = false;
						NodeStatus nodeResponder = null;
						
						Thread.sleep(3000);
						for (NodeStatus hostName : listNodes) {
							path = ZkConf.DATANODES_PATH + "/" + hostName.getHostname();
							byte[] bytes = zooKeeper.getData(path,	true, null);
							if(bytes != null){
								NodeStatus datanode = (NodeStatus) Serializer.toObject(bytes);
								nodes.add(datanode);
							}
						}
					
						int lesser;
						int nearest = idLastNode;
						for(NodeStatus nextNodeList: nodes){
							if(nextNodeList.getHostNameResponding().equals(hostNameDesc)){
								nodeResponding = true;
							}
							
							int id = getIdDatanode(nextNodeList.getHostname());
							if(id != idNextNode){
								if (id > idNodeDesconnected){
									lesser = id - idNodeDesconnected;
								} else{
									lesser = idNodeDesconnected - id;
								}
								
								if(lesser < nearest){
									nearest = lesser;
									nodeResponder = nextNodeList;
								}
							}
						}
						
						if((nodeResponding == false) && 
								(nodeResponder.getHostname().equals(node.getHostname()))){		
							path = ZkConf.DATANODES_PATH + "/" + node.getHostname();
							node.setTwoResponding(true);
							node.setHostNameResponding(hostNameDesc);
							zooKeeper.setData(path, Serializer.fromObject(node), -1);
						}
						
					}
				}
			}
			
		} catch (KeeperException e) {
			e.getMessage();
		} catch (InterruptedException e) {
			e.getMessage();
		} catch (IOException e) {
			e.getMessage();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized static void managerNodesChanged(String path, ZooKeeper zooKeeper) {
		try{
			byte[] bytes = zooKeeper.getData(path,	true, null);
			if(bytes != null){
				NodeStatus datanode = (NodeStatus) Serializer.toObject(bytes);
				if(managerNodesResponding.containsKey(datanode.getHostname())){
					managerNodesResponding.remove(datanode.getHostname());
				} else{
					managerNodesResponding.put(datanode.getHostname(), datanode.getHostNameResponding());					
				}
			}
			
		} catch (KeeperException e) {
			e.getMessage();
		} catch (InterruptedException e) {
			e.getMessage();
		} catch (IOException e) {
			e.getMessage();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
	}
	
	public synchronized static void managerNodesConnected(String hostName, NodeStatus node, ZooKeeper zooKeeper) {
		try{
			String path = ZkConf.DATANODES_PATH + node.getHostname();
			
			if(nodesDesconnected.contains(hostName)){
				if(hostName.equals(node.getHostNameResponding())){
					nodesDesconnected.remove(nodesDesconnected.indexOf(hostName));
					node.setTwoResponding(false);
					node.setHostNameResponding("");
					zooKeeper.setData(path, Serializer.fromObject(node), -1);
				} else{
					nodesDesconnected.remove(nodesDesconnected.indexOf(hostName));
				}
			}
		
		} catch (KeeperException e) {
			e.getMessage();
		} catch (InterruptedException e) {
			e.getMessage();
		} catch (IOException e) {
			e.getMessage();
		} 
	}
	
	public static void loadMemoryHistoricNodes() {
		nodesHistoric = ClusterService.getHistoricDataNodes();
		for(String hostName: nodesHistoric){
			String path = ZkConf.DATANODES_PATH + "/" + hostName;
			if(historic.containsKey(path))
				historic.put(path, hostName);
		}
	}
	
	public static List<NodeStatus> getDataNodes(){
		if (nodesConnected.isEmpty())
			loadRoundRobinPartitionPolicy();
		
		return nodesConnected;
	}
	
	public static List<NodeStatus> getNodesReplication(){
		if (nodesReplySend.isEmpty())
			loadNodesReplySend();
		
		return nodesReplySend;
	}
	
	public static Map<String, String> getHistoric() {
		return historic;
	}

	public static NodeStatus nextNode() {
		return partitionPolicy.nextNode();
	}
	
	private static int getIdDatanode(String hostname){
		int underline = hostname.lastIndexOf("_");
		int end = hostname.length();
		
		String identificador = hostname.substring(underline + 1, end);
		int id = Integer.valueOf(identificador);
		return id;
	}
	
	private static List<NodeStatus> getListNodeSendReply(String hostName){
		List<NodeStatus> nodes = new ArrayList<NodeStatus>();
		int idFirstNode = 1;		
		
		int idNode = getIdDatanode(hostName);
		for(int i=0; i < replyFrequency; i++){
			NodeStatus node = null;
			while(node == null){
				idNode++;
				if(idNode > idLastNode)
					idNode = idFirstNode;
					
				node = mapReplyUtil.get(idNode);
					
				if(node != null ) 
					nodes.add(node);
			}
		}
		return nodes;		
	}
}
