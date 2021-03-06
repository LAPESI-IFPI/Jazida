package br.edu.ifpi.jazida.node.replication;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ConcurrentModificationException;
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
import br.edu.ifpi.jazida.node.protocol.IImageReplicationProtocol;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.ListsManager;
import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class ImageReplicationNode {
	private static final Logger LOG = Logger.getLogger(ImageReplicationNode.class);
	private static Configuration HADOOP_CONFIGURATION = new Configuration();
	private static ImageReplicationNode imageReplicationNode = new ImageReplicationNode();
	private Map<String, IImageReplicationProtocol> proxyMap = new HashMap<String, IImageReplicationProtocol>();
	private List<NodeStatus> datanodes;
	private ExecutorService threadPool;
	private final Text hostname = new Text(DataNodeConf.DATANODE_HOSTNAME);
	private final Text IP = new Text(DataNodeConf.DATANODE_HOSTADDRESS);
	
	public ImageReplicationNode(){		
	}
	
	public static ImageReplicationNode getImageReplicationNodeUtil() {
		if (imageReplicationNode == null) {
			imageReplicationNode = new ImageReplicationNode();
		}
		return imageReplicationNode;
	}
	
	private void loadProxy(){
		try{
			datanodes = ListsManager.getDatanodesReplication();
			if (datanodes.size() == 0){
				LOG.info("Apenas um datanode conectado ao cluster");
			}
			else {
				for (NodeStatus node : datanodes) {
					final InetSocketAddress socketAdress = new InetSocketAddress(node.getAddress(), node.getImageReplicationServerPort());
					IImageReplicationProtocol replicationProxy = this.getImageReplicationServer(socketAdress);
					if(replicationProxy != null)
						proxyMap.put(node.getHostname(), replicationProxy);
				}
			threadPool = Executors.newCachedThreadPool();
			}
		
		} catch (ConcurrentModificationException e) {
			LOG.info("Reordenando listas.");
		}
	}
	
	private IImageReplicationProtocol getImageReplicationServer(final InetSocketAddress endereco) {
		try{
			IImageReplicationProtocol proxy = (IImageReplicationProtocol) RPC.getProxy(
					IImageReplicationProtocol.class, IImageReplicationProtocol.versionID,
					endereco, HADOOP_CONFIGURATION);
			return proxy;
		} catch (IOException e){
			LOG.error(e.fillInStackTrace(), e);
		}
		return null;
	}
	
	public void addImageReply(final MetaDocumentWritable metaDocWrapper,
			final BufferedImageWritable image, final LongWritable numDocsIndex) throws KeeperException, InterruptedException, IOException {		
		loadProxy();
		try {
			if (datanodes.size() > 0){
				LOG.info("Enviando imagem para os índices das Réplicas...");
				for (final NodeStatus nodeStatus : datanodes) {
					LOG.info("Enviando para: " + nodeStatus.getHostname());
					Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
					@Override
					public IntWritable call() throws Exception {
						IImageReplicationProtocol proxy = proxyMap.get(nodeStatus.getHostname());
						return proxy.addImageReply(metaDocWrapper, image, hostname, IP, numDocsIndex);
						}
					});
					
					IntWritable returnCode = request.get(3500, TimeUnit.MILLISECONDS);
					if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.SUCCESS){
						LOG.info("Enviado com sucesso.");
					} else if (ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.UNEXPECTED_INDEX_ERROR){
						LOG.info("A réplica de imagem do "+ nodeStatus.getHostname() + " precisou ser restaurada.");
					} else if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.OUTDATED){
						LOG.info("A réplica de imagem do "+ nodeStatus.getHostname() + " precisou ser atualizada.");
					}	
						
				}
				
			}
		
		} catch (ConcurrentModificationException e) {
			LOG.info("Reordenando listas.");
		} catch (InterruptedException e) {
			LOG.error(e.fillInStackTrace(), e);
		} catch (ExecutionException e) {
			LOG.info("Reordenando listas.");
		} catch (Throwable e){
			LOG.error(e.fillInStackTrace(), e);
		}
	}

	public void delImageReply(final Text id) throws KeeperException, InterruptedException, IOException {		
		
		loadProxy();
		try {
			if (datanodes.size() > 0){
				LOG.info("Deletando imagem nos índices das Réplicas...");
				for (final NodeStatus nodeStatus : datanodes) {
					LOG.info("Deletando em: " + nodeStatus.getHostname());
					Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
						@Override
						public IntWritable call() throws Exception {
							IImageReplicationProtocol proxy = proxyMap.get(nodeStatus.getHostname());
							return proxy.delImageReply(id, hostname, IP);
						}
					});
					
					IntWritable returnCode = request.get(3500, TimeUnit.MILLISECONDS);
					if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.SUCCESS){
						LOG.info("Deletado com sucesso.");
					} else if (ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.UNEXPECTED_INDEX_ERROR){
						LOG.info("A réplica de imagem do "+ nodeStatus.getHostname() + " precisou ser restaurada.");
					}
					
				}
				
			}
		
		} catch (ConcurrentModificationException e) {
			LOG.info("Reordenando listas.");
		} catch (InterruptedException e) {
			LOG.error(e.fillInStackTrace(), e);
		} catch (ExecutionException e) {
			LOG.info("Reordenando listas.");
		} catch (TimeoutException e) {
			LOG.error(e.fillInStackTrace(), e);
		} catch (Throwable e){
			LOG.error(e.fillInStackTrace(), e);
		}
	}

	public void updateImageReply(final Text id, final MapWritable mapMetaDocument) throws KeeperException, InterruptedException, IOException {		
		loadProxy();
		try {
			if (datanodes.size() > 0){
				LOG.info("Atualizando imagem nos índices das Réplicas...");
				for (final NodeStatus nodeStatus : datanodes) {
					LOG.info("Atualizando em: " + nodeStatus.getHostname());
					Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
						@Override
						public IntWritable call() throws Exception {
							IImageReplicationProtocol proxy = proxyMap.get(nodeStatus.getHostname());
							return proxy.updateImageReply(id, mapMetaDocument, hostname, IP);
						}
					});
					
					IntWritable returnCode = request.get(3500, TimeUnit.MILLISECONDS);
					if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.SUCCESS){
						LOG.info("Atualizado com sucesso.");
					} else if (ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.UNEXPECTED_INDEX_ERROR){
						LOG.info("A réplica de imagem do "+ nodeStatus.getHostname() + " precisou ser restaurada.");
					}
					
				}
					
			}
		
		} catch (ConcurrentModificationException e) {
			LOG.info("Reordenando listas.");
		} catch (InterruptedException e) {
			LOG.error(e.fillInStackTrace(), e);
		} catch (ExecutionException e) {
			LOG.info("Reordenando listas.");
		} catch (TimeoutException e) {
			LOG.error(e.fillInStackTrace(), e);
		} catch (Throwable e){
			LOG.error(e.fillInStackTrace(), e);
		}
	}

}
