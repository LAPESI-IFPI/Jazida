package br.edu.ifpi.jazida.client;

import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.exception.NoNodesAvailableException;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.protocol.IImageIndexerProtocol;
import br.edu.ifpi.jazida.util.ListsManager;
import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.jazida.writable.WritableUtils;
import br.edu.ifpi.opala.indexing.ImageIndexer;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class ImageIndexerClient implements ImageIndexer {

	private static final Logger LOG = Logger.getLogger(ImageIndexerClient.class);
	private static Configuration HADOOP_CONFIGURATION = new Configuration();
	private HashMap<String,IImageIndexerProtocol> proxyMap = new HashMap<String, IImageIndexerProtocol>();
	private ExecutorService threadPool;
	List<NodeStatus> datanodes;

	public ImageIndexerClient() throws KeeperException, InterruptedException, IOException {
		threadPool = Executors.newCachedThreadPool();
	}
	
	private void loadProxy() {
		datanodes = ListsManager.getDataNodes();
		if (datanodes.size() == 0)
			throw new NoNodesAvailableException("Nenhum DataNode conectado ao ClusterService.");
		
		for (NodeStatus node : datanodes) {
			final InetSocketAddress socketAdress = new InetSocketAddress(node.getAddress(),
																		 node.getImageIndexerServerPort());
			IImageIndexerProtocol imageIndexerProtocol = getImageIndexerProxy(socketAdress);
			
			if(imageIndexerProtocol != null){
				proxyMap.put(node.getHostname(), imageIndexerProtocol);	
			}
		}
	}
	
	private IImageIndexerProtocol getImageIndexerProxy(final InetSocketAddress endereco) {
		
		try{
			IImageIndexerProtocol proxy = (IImageIndexerProtocol) RPC.getProxy(
											IImageIndexerProtocol.class,
											IImageIndexerProtocol.versionID,
											endereco, HADOOP_CONFIGURATION);
			return proxy;
		}catch (IOException e){
			LOG.info(e);
		}
		
		return null;
	}

	@Override
	public ReturnMessage addImage(MetaDocument metaDocument, BufferedImage image) {
		
		loadProxy();
		try{
			NodeStatus node = ListsManager.nextNode();
	
			IImageIndexerProtocol proxy = proxyMap.get(node.getHostname());
			IntWritable result = proxy.addImage(new MetaDocumentWritable(metaDocument),
												new BufferedImageWritable(image));
			
			LOG.info("Indexação da imagem "+metaDocument.getId()+" em "+node.getHostname()
					+" retornou "+ReturnMessage.getReturnMessage(result.get()));
			
			return ReturnMessage.getReturnMessage(result.get());
			
		} catch (Throwable e){
			e.getStackTrace();
		}
		
		return ReturnMessage.ID_NOT_FOUND;
	}

	@Override
	public ReturnMessage delImage(final String id) {
		ReturnMessage message = ReturnMessage.ID_NOT_FOUND;
		
		loadProxy();
		try {
			ArrayList<Future<IntWritable>> requests = new ArrayList<Future<IntWritable>>();
			for (final NodeStatus nodeStatus : datanodes) {
				Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
					@Override
					public IntWritable call() throws Exception {
						IImageIndexerProtocol proxy = proxyMap.get(nodeStatus.getHostname());
						return proxy.delImage(new Text(id));
					}
				});
				requests.add(request);
			}
			for (Future<IntWritable> future : requests) {
				IntWritable returnCode = future.get(3000, TimeUnit.MILLISECONDS);
				if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.SUCCESS){
					message = ReturnMessage.SUCCESS;
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
		return message;
	}


	public void backupNow() throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub

	}


	public void restoreBackup() {
		// TODO Auto-generated method stub

	}

	@Override
	public ReturnMessage updateImage(final String id, final Map<String, String> metaDocument) {
		ReturnMessage message = ReturnMessage.ID_NOT_FOUND;
		
		loadProxy();
		try {
			ArrayList<Future<IntWritable>> requests = new ArrayList<Future<IntWritable>>();
			for (final NodeStatus nodeStatus : datanodes) {
				Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
					@Override
					public IntWritable call() throws Exception {
						IImageIndexerProtocol proxy = proxyMap.get(nodeStatus.getHostname());
						return proxy.updateImage(new Text(id), WritableUtils.convertMapToMapWritable(metaDocument));
					}
				});
				requests.add(request);
			}
			for (Future<IntWritable> future : requests) {
				IntWritable returnCode = future.get(3000, TimeUnit.MILLISECONDS);
				if(ReturnMessage.getReturnMessage(returnCode.get()) == ReturnMessage.SUCCESS) {
					message = ReturnMessage.SUCCESS;
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
		return message;
	}

	@Override
	public void restoreIndex() {
		// TODO Auto-generated method stub
		
	}

}
