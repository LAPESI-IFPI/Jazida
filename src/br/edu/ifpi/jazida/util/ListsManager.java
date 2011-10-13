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
	private static List<NodeStatus> datanodesConnected = new ArrayList<NodeStatus>();
	private static List<String> datanodesDesconnected = new ArrayList<String>();
	private static List<NodeStatus> datanodesReplySend = new ArrayList<NodeStatus>();
	private static Map<Integer, NodeStatus> mapReplyUtil = new HashMap<Integer, NodeStatus>();
	private static Map<Integer, NodeStatus> cacheMapReplyUtil = new HashMap<Integer, NodeStatus>();
	private static List<NodeStatus> datanodesReplyReceive = new ArrayList<NodeStatus>();
	private static List<String> cacheDatanodesReplyReceive = new ArrayList<String>();
	private static Map<String, List<String>> managerDatanodesResponding = new HashMap<String, List<String>>();
	private static Map<String, List<String>> historicSendDatanodesDesconnected = new HashMap<String, List<String>>();
	private static int idLastNode = 0;
	private static int idFirstNode = 9999999;
	private static int replyFrequency;
	private static int cacheReplyFrequency;
	
	private synchronized static void loadRoundRobinPartitionPolicy(){
		if (!datanodesConnected.isEmpty())
			datanodesConnected.clear();
		
		datanodesConnected = getDatanodesConnected();
		partitionPolicy.clear();
		partitionPolicy.addNodes(datanodesConnected.toArray(new NodeStatus[datanodesConnected.size()]));
	}	

	private synchronized static void loadDatanodesReplySend() {
		if(!datanodesReplySend.isEmpty())
			datanodesReplySend.clear();		
		
		int idNode = getIdDatanode(HOSTNAME);
		for(int i=0; i < replyFrequency; i++){
			NodeStatus node = null;
			if(datanodesConnected.size() > 1){
				while(node == null){
					idNode++;
					if(idNode > idLastNode)
						idNode = idFirstNode;
					
					node = mapReplyUtil.get(idNode);
					
					if(node != null ) 
						datanodesReplySend.add(node);
				}
			}
		}		
	}
	
	private synchronized static void loadDatanodesReplyReceive() {
		if(!datanodesReplyReceive.isEmpty())
			datanodesReplyReceive.clear();
	
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
					datanodesReplyReceive.add(node);
			}
		}
	}
	
	private synchronized static void setUpSendReceiveReply(){
		if(datanodesConnected.isEmpty())
			loadRoundRobinPartitionPolicy();
		
		if(datanodesConnected.size() <= REPLY_FREQUENCY){
			replyFrequency = (datanodesConnected.size() - 1);
		} else {
			replyFrequency = REPLY_FREQUENCY;
		}
		
		if(!mapReplyUtil.isEmpty())
			mapReplyUtil.clear();

		for(NodeStatus node: datanodesConnected){
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
		
		if(!cacheDatanodesReplyReceive.isEmpty()){ 
			cacheDatanodesReplyReceive.clear();
		}		
		for(NodeStatus node: datanodesReplyReceive){
			cacheDatanodesReplyReceive.add(node.getHostname());
		}		
	}
	
	public static void manager(){
		loadRoundRobinPartitionPolicy();
		loadCache();
		setUpSendReceiveReply();
		loadDatanodesReplySend();
		loadDatanodesReplyReceive();
	}
	
	public synchronized static void managerDatanodesDeleted(String hostNameDesc, NodeStatus nodeLocal) {
		try{
			
			if(cacheDatanodesReplyReceive.contains(hostNameDesc)){
				String path;
				NodeStatus leader = null;
				
				loadDada();
				
				if(!datanodesDesconnected.contains(hostNameDesc)){
					datanodesDesconnected.add(hostNameDesc);					
				}
				
				List<String> listSend = getListNodeSendReply(hostNameDesc);
				if(!historicSendDatanodesDesconnected.containsKey(hostNameDesc)){
					historicSendDatanodesDesconnected.put(hostNameDesc, listSend);
				}
				
				leader = getDatanodeLeader(hostNameDesc);
				if(HOSTNAME.equals(leader.getHostname())){
					zk.setData(ZkConf.DATANODES_DESCONNECTED, Serializer.fromObject((Serializable) datanodesDesconnected), -1);
					zk.setData(ZkConf.HISTORIC_SEND, Serializer.fromObject((Serializable) historicSendDatanodesDesconnected), -1);
				}
				
				if(!managerDatanodesResponding.containsKey(hostNameDesc)){
					List<NodeStatus> nodesExists = null;
					NodeStatus nodeResponder = null;
					
					nodesExists = getDatanodesExists(listSend);
					nodeResponder = getDatanodeResponder(nodesExists, hostNameDesc);
					
					if(nodeResponder != null){
						if(nodeResponder.getHostname().equals(HOSTNAME)){
							path = ZkConf.DATANODES_PATH + "/" + HOSTNAME;
							nodeLocal.getNodesResponding().add(hostNameDesc);
							try {
								zk.setData(path, Serializer.fromObject(nodeLocal), -1);
							} catch (KeeperException e) {}
						}
					}
					
				} else {
					
					List<NodeStatus> nodesExists = null;
					NodeStatus nodeResponder = null;
					
					nodesExists = getDatanodesExists(listSend);
					nodeResponder = getDatanodeResponder(nodesExists, hostNameDesc);
					
					if(nodeResponder != null){
						if(nodeResponder.getHostname().equals(HOSTNAME)){
							path = ZkConf.DATANODES_PATH + "/" + HOSTNAME;
							nodeLocal.getNodesResponding().add(hostNameDesc);
							try {
								zk.setData(path, Serializer.fromObject(nodeLocal), -1);
							} catch (KeeperException e) {}
						}
					}
					
					Thread.sleep(3500);
					nodesExists.clear();					
					
					List<String> listNodesResponding = managerDatanodesResponding.get(hostNameDesc);
					List<String> listHistoric = new ArrayList<String>();
					
					for(String nodeName: listNodesResponding){
						listHistoric = historicSendDatanodesDesconnected.get(nodeName);
						
						nodesExists = getDatanodesExists(listHistoric);
						nodeResponder = getDatanodeResponder(nodesExists, nodeName);
						
						if((nodeResponder != null) && (!nodeResponder.getHostname().equals(""))){
							path = ZkConf.DATANODES_PATH + "/" + nodeResponder.getHostname();
							nodeResponder.getNodesResponding().add(nodeName);
							try {
								zk.setData(path, Serializer.fromObject(nodeResponder), -1);	
							} catch (KeeperException e) {}
						}
					}
				}
				
				if(managerDatanodesResponding.containsKey(hostNameDesc)){
					managerDatanodesResponding.remove(hostNameDesc);
				}
				
				if(HOSTNAME.equals(leader.getHostname())){
					zk.setData(ZkConf.MANAGER_NODES_RESPONDING, Serializer.fromObject((Serializable) managerDatanodesResponding), -1);
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
	
	public synchronized static void managerNodesChanged(String hostName, NodeStatus nodeLocal) {
		try{
			
			// Verificar aqui !!!
			System.out.println("managerNodesChanged---------------------");
			System.out.println("historicSendDatanodesDesconnected: " + historicSendDatanodesDesconnected);
			System.out.println("getHistoricSendDatanodesDesconnected(): " + getHistoricSendDatanodesDesconnected());
			System.out.println("managerDatanodesResponding:  " + managerDatanodesResponding);
			System.out.println("getManagerDatanodesResponding():  " + getManagerDatanodesResponding());
			System.out.println("nodesDesconnected: " + datanodesDesconnected);
			System.out.println("getDatanodesDisconnected(): " + getDatanodesDisconnected());
			System.out.println("managerNodesChanged---------------------");
			
			if(hostName.equals(HOSTNAME)){
				String path = ZkConf.DATANODES_PATH + "/" + HOSTNAME;
				managerDatanodesResponding = getManagerDatanodesResponding();
				
				byte[] bytes = zk.getData(path,	true, null);
				List<String> nodesResponding = new ArrayList<String>();
				NodeStatus datanode = (NodeStatus) Serializer.toObject(bytes);
				
				managerDatanodesResponding.put(datanode.getHostname(), datanode.getNodesResponding());
				nodesResponding = managerDatanodesResponding.get(datanode.getHostname());
				if(nodesResponding.isEmpty())
					managerDatanodesResponding.remove(datanode.getHostname());
				
				nodeLocal.setNodesResponding(datanode.getNodesResponding());
				zk.setData(ZkConf.MANAGER_NODES_RESPONDING, Serializer.fromObject((Serializable) managerDatanodesResponding), -1);
				
			}
			
			if(nodeLocal.getNodesResponding().size() > 0){
				LOG.info("Este datanode esta respondendo pelo(s): " + nodeLocal.getNodesResponding());
			}
			
			clear();
			System.out.println();
			System.out.println("managerNodesChanged----------------");
			System.out.println("historicSendNodesDesconnected: " + historicSendDatanodesDesconnected);
			System.out.println("getHistoricSendDatanodesDesconnected(): " + getHistoricSendDatanodesDesconnected());
			System.out.println("nodesDesconnected: " + datanodesDesconnected);
			System.out.println("getDatanodesDisconnected(): " + getDatanodesDisconnected());
			System.out.println("managerDatanodesResponding:  " + managerDatanodesResponding);
			System.out.println("getManagerDatanodesResponding():  " + getManagerDatanodesResponding());
			System.out.println("respondendo: " + nodeLocal.getNodesResponding());
			System.out.println("managerNodesChanged----------------");
			
		
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

	public synchronized static void managerDatanodesCreated(String hostName, NodeStatus nodeLocal) {
		try{
			
			System.out.println("managerDatanodesCreated---------------------");
			System.out.println("historicSendNodesDesconnected: " + historicSendDatanodesDesconnected);
			System.out.println("getHistoricSendDatanodesDesconnected(): " + getHistoricSendDatanodesDesconnected());
			System.out.println("managerDatanodesResponding:  " + managerDatanodesResponding);
			System.out.println("getManagerDatanodesResponding():  " + getManagerDatanodesResponding());
			System.out.println("nodesDesconnected: " + datanodesDesconnected);
			System.out.println("getDatanodesDisconnected(): " + getDatanodesDisconnected());
			System.out.println("managerDatanodesCreated---------------------");
			
			
			if(nodeLocal.getNodesResponding().contains(hostName)){
				String path = ZkConf.DATANODES_PATH + "/" + HOSTNAME;				
				nodeLocal.getNodesResponding().remove(nodeLocal.getNodesResponding().indexOf(hostName));
				zk.setData(path, Serializer.fromObject(nodeLocal), -1);			
			}
			
			if(HOSTNAME.equals(hostName)){
				loadDada();
				
				if(managerDatanodesResponding.containsKey(hostName)){
					managerDatanodesResponding.remove(hostName);
					zk.setData(ZkConf.MANAGER_NODES_RESPONDING, Serializer.fromObject((Serializable) managerDatanodesResponding), -1);
				}
				
				if(datanodesDesconnected.contains(hostName)){
					datanodesDesconnected.remove(hostName);
					
					if (historicSendDatanodesDesconnected.containsKey(hostName)){
						historicSendDatanodesDesconnected.remove(hostName);
					}
					
					zk.setData(ZkConf.DATANODES_DESCONNECTED, Serializer.fromObject((Serializable) datanodesDesconnected), -1);					
					zk.setData(ZkConf.HISTORIC_SEND, Serializer.fromObject((Serializable) historicSendDatanodesDesconnected), -1);
					
					Thread.sleep(2000);
					if(hostName.equals(HOSTNAME)){
						new SupportReplyText().checkRepliesText(datanodesReplyReceive);
						new SupportReplyImage().checkRepliesImage(datanodesReplyReceive);
					}
				} 	
				
				clear();
				System.out.println();
				System.out.println("managerDatanodesCreated---------------------");
				System.out.println("historicSendDatanodesDesconnected: " + historicSendDatanodesDesconnected);
				System.out.println("getHistoricSendDatanodesDesconnected(): " + getHistoricSendDatanodesDesconnected());
				System.out.println("managerDatanodesResponding:  " + managerDatanodesResponding);
				System.out.println("getManagerDatanodesResponding():  " + getManagerDatanodesResponding());
				System.out.println("nodesDesconnected: " + datanodesDesconnected);
				System.out.println("getDatanodesDisconnected(): " + getDatanodesDisconnected());
				System.out.println("managerDatanodesCreated----------------------");
				System.out.println();
				
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
	
	public static List<NodeStatus> getDatanodes(){
		if (datanodesConnected.isEmpty())
			loadRoundRobinPartitionPolicy();
		
		return datanodesConnected;
	}
	
	public static List<NodeStatus> getDatanodesReplication(){
		if (datanodesReplySend.isEmpty()){
			setUpSendReceiveReply();
			loadDatanodesReplySend();
		}
		
		return datanodesReplySend;
	}
	
	public static NodeStatus nextDatanode() {
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
		List<String> datanodes = new ArrayList<String>();
		
		try {
			byte[] bytes = zk.getData(ZkConf.DATANODES_DESCONNECTED, false, null);
			if (bytes != null)
				datanodes = (List<String>) Serializer.toObject(bytes);
		} catch (KeeperException e) {
			LOG.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} 
	
		return datanodes;
	}
	
	private static NodeStatus getDatanodeLeader(String hostNameDesc) throws KeeperException, InterruptedException, IOException, ClassNotFoundException{
		String path;
		NodeStatus datanode = null;
		List<String> list = historicSendDatanodesDesconnected.get(hostNameDesc);
		for(String nameLeader: list) {
			path = ZkConf.DATANODES_PATH + "/" + nameLeader;
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
	
	private static NodeStatus getDatanodeResponder (List<NodeStatus> datanodes, String hostName){
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
	
	private static void loadDada() {
		try {
			
			managerDatanodesResponding = getManagerDatanodesResponding();
			historicSendDatanodesDesconnected = getHistoricSendDatanodesDesconnected();
			datanodesDesconnected = getDatanodesDesconnected();			
			
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
	
	private static int getIdDatanode(String hostname){
		int underline = hostname.lastIndexOf("_");
		int end = hostname.length();
		
		String identificador = hostname.substring(underline + 1, end);
		return Integer.valueOf(identificador);
	}
	
	public static void setZk(ZooKeeper zk) {
		ListsManager.zk = zk;
	}
	
	private static Map<String, List<String>> getManagerDatanodesResponding() throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
		Map<String, List<String>> managerDatanodesResponding = new HashMap<String, List<String>>();
		byte[] bytes =  zk.getData(ZkConf.MANAGER_NODES_RESPONDING, false, null);
		if(bytes != null) {
			managerDatanodesResponding = ((Map<String, List<String>>) Serializer.toObject(bytes));
		}
		
		return managerDatanodesResponding;
	}

	private static Map<String, List<String>> getHistoricSendDatanodesDesconnected() throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
		Map<String, List<String>> historicSendNodesDesconnected = new HashMap<String, List<String>>();
		byte[] bytes =  zk.getData(ZkConf.HISTORIC_SEND, false, null);
		if(bytes != null) {
			historicSendNodesDesconnected = (Map<String, List<String>>) Serializer.toObject(bytes);			
		}
		
		return historicSendNodesDesconnected;
	}

	private static List<String> getDatanodesDesconnected() throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
		List<String> nodesDesconnected = new ArrayList<String>();
		byte[] bytes =  zk.getData(ZkConf.DATANODES_DESCONNECTED, false, null);
		if(bytes != null) {
			nodesDesconnected = (List<String>) Serializer.toObject(bytes);			
		}
		
		return nodesDesconnected;
	}
	
	private static void clear(){
		if(!historicSendDatanodesDesconnected.isEmpty()){
			historicSendDatanodesDesconnected.clear();
		}
		
		if(!managerDatanodesResponding.isEmpty()){
			managerDatanodesResponding.clear();
		}
		
		if(!datanodesDesconnected.isEmpty()){
			datanodesDesconnected.clear();
		}
	}
	
}
