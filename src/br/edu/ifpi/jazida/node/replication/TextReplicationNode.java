package br.edu.ifpi.jazida.node.replication;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.protocol.ITextReplicationProtocol;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.ListsManager;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class TextReplicationNode {

	private static final Logger LOG = Logger.getLogger(TextReplicationNode.class);
	private static Configuration HADOOP_CONFIGURATION = new Configuration();
	private static TextReplicationNode textReplicationNode = new TextReplicationNode();
	private Map<String, ITextReplicationProtocol> proxyMap = new HashMap<String, ITextReplicationProtocol>();
	private List<NodeStatus> datanodes;
	private ExecutorService threadPool;
	private final Text hostname = new Text(DataNodeConf.DATANODE_HOSTNAME);
	private final Text IP = new Text(DataNodeConf.DATANODE_HOSTADDRESS);
	
	public TextReplicationNode(){
	}
	
	public static TextReplicationNode getTextReplicationNodeUtil() {
		if (textReplicationNode == null) {
			textReplicationNode = new TextReplicationNode();
		}
		return textReplicationNode;
	}
	
	private void loadProxy(){
		datanodes = ListsManager.getNodesReplication();
		if (datanodes.size() == 0){
			LOG.info("Apenas um datanode conectado ao cluster");
		}
		else{
			for (NodeStatus node : datanodes) {
				final InetSocketAddress socketAdress = new InetSocketAddress(node.getAddress(), node.getTextReplicationServerPort());
				ITextReplicationProtocol replicationProxy = this.getTextReplicationServer(socketAdress);
				proxyMap.put(node.getHostname(), replicationProxy);
			}
			threadPool = Executors.newCachedThreadPool();
		}
	}

	private ITextReplicationProtocol getTextReplicationServer(final InetSocketAddress endereco) {
		
		try{
			ITextReplicationProtocol proxy = (ITextReplicationProtocol) RPC.getProxy(
					ITextReplicationProtocol.class, ITextReplicationProtocol.versionID,
					endereco, HADOOP_CONFIGURATION);
			
			return proxy;
		} catch(IOException e){
			e.getMessage();
		}
		
		return null;
	}
	
	public void addTextReply(final MetaDocumentWritable metaDocWrapper, final Text content, final LongWritable numDocsIndex) throws KeeperException, InterruptedException, IOException {		
		loadProxy();
		try {
			if (datanodes.size() > 0){
				ArrayList<Future<IntWritable>> requests = new ArrayList<Future<IntWritable>>();
				LOG.info("Enviando o documento para os índices das Réplicas...");
				for (final NodeStatus nodeStatus : datanodes) {
						LOG.info("Enviando para: " + nodeStatus);
						Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
							@Override
							public IntWritable call() throws Exception {
								ITextReplicationProtocol proxy = proxyMap.get(nodeStatus.getHostname());
								return proxy.addTextReply(metaDocWrapper, content, hostname, IP, numDocsIndex);
							}
						});
						requests.add(request);
				}
				for (Future<IntWritable> future : requests) {
					IntWritable returnCode = future.get();
					if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.UNEXPECTED_INDEX_ERROR){
						LOG.info("Uma réplica precisou ser restaurada.");
					}
					if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.OUTDATED){
						LOG.info("Uma réplica precisou ser atualizada.");
					}
				}					
			}
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (ExecutionException e) {
			LOG.error(e);
		} catch (Throwable e){
			LOG.error(e);
		}
	}

	public void delTextReply(final Text identifier) throws KeeperException, InterruptedException, IOException {
		loadProxy();
		try{
			if (datanodes.size() > 0){
				ArrayList<Future<IntWritable>> requests = new ArrayList<Future<IntWritable>>();
				LOG.info("Deletando o documento nos índices das Réplicas...");
				for (final NodeStatus nodeStatus : datanodes) {
					LOG.info("Enviando para: " + nodeStatus);
					Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
						@Override
						public IntWritable call() throws Exception {
							ITextReplicationProtocol proxy = proxyMap.get(nodeStatus.getHostname());
							return proxy.delTextReply(identifier, hostname, IP);
						}
					});
					requests.add(request);
				}
				for (Future<IntWritable> future : requests) {
					IntWritable returnCode = future.get(3000, TimeUnit.MILLISECONDS);
					if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.UNEXPECTED_INDEX_ERROR){
						LOG.info("Uma réplica precisou ser restaurada.");
					}
				}					
			}
		
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (ExecutionException e) {
			LOG.error(e);
		} catch (TimeoutException e) {
			LOG.error(e);
		} catch (Throwable e){
			LOG.error(e);
		}
		
	}

	public void updateTextReply(final Text identifier, final MapWritable updatesWritable) throws KeeperException, InterruptedException, IOException {
		loadProxy();
		try{
			if (datanodes.size() > 0){
				ArrayList<Future<IntWritable>> requests = new ArrayList<Future<IntWritable>>();
				LOG.info("Atualizando o documento nos índices das Réplicas...");
				for (final NodeStatus nodeStatus : datanodes) {
					LOG.info("Enviando para: " + nodeStatus);
					Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
							@Override
							public IntWritable call() throws Exception {
								ITextReplicationProtocol proxy = proxyMap.get(nodeStatus.getHostname());
								return proxy.updateTextReply(identifier, updatesWritable, hostname, IP);
							}
						});
						requests.add(request);
				}
				for (Future<IntWritable> future : requests) {
					IntWritable returnCode = future.get(3000, TimeUnit.MILLISECONDS);
					if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.UNEXPECTED_INDEX_ERROR){
						LOG.info("Uma réplica precisou ser restaurada.");
					}
				}		
			}
		
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (ExecutionException e) {
			LOG.error(e);
		} catch (TimeoutException e) {
			LOG.error(e);
		} catch (Throwable e){
			LOG.error(e);
		}
		
	}

}
