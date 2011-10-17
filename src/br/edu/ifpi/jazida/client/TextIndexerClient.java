package br.edu.ifpi.jazida.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.exception.NoNodesAvailableException;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.protocol.ITextIndexerProtocol;
import br.edu.ifpi.jazida.util.ListsManager;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.jazida.writable.WritableUtils;
import br.edu.ifpi.opala.indexing.TextIndexer;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;


/**
 * Interface para usuário do Jazida. Aplicações devem utilizar essa classe para
 * comunicação com um cluster Jazida.
 * 
 * @author Aécio Santos
 * 
 */
public class TextIndexerClient implements TextIndexer {

	private static final Logger LOG = Logger.getLogger(TextIndexerClient.class);
	private static Configuration HADOOP_CONFIGURATION = new Configuration();
	private Map<String, ITextIndexerProtocol> proxyMap = new HashMap<String, ITextIndexerProtocol>();
	private ExecutorService threadPool;
	private List<NodeStatus> datanodes;

	public TextIndexerClient() throws KeeperException, InterruptedException, IOException {
		threadPool = Executors.newCachedThreadPool();
	}

	private void loadProxy() {
		try{
			datanodes = ListsManager.getDatanodes();
			if (datanodes.size() == 0)
				throw new NoNodesAvailableException("Nenhum DataNode conectado ao ClusterService.");
			
			for (NodeStatus node : datanodes) {
				final InetSocketAddress socketAdress = new InetSocketAddress(node.getAddress(),
																			 node.getTextIndexerServerPort());
				ITextIndexerProtocol textIndexerProxy = getTextIndexerServer(socketAdress);
				
				if(textIndexerProxy != null){
					proxyMap.put(node.getHostname(), textIndexerProxy);	
				}
			}
		} catch (ConcurrentModificationException e) {
			LOG.info("Reordenando listas.");
		}
	}

	private ITextIndexerProtocol getTextIndexerServer(final InetSocketAddress endereco) {
		try{
			ITextIndexerProtocol proxy = (ITextIndexerProtocol) RPC.getProxy(
												ITextIndexerProtocol.class,
												ITextIndexerProtocol.versionID,
												endereco, HADOOP_CONFIGURATION);
			return proxy;
		
		} catch (IOException e){
			e.getStackTrace();
		}
		
		return null;
	}
	
	@Override
	public ReturnMessage addText(MetaDocument metaDocument, String content) {
		IntWritable result = null;
		try{
			
			loadProxy();
			
			MetaDocumentWritable documentWrap = new MetaDocumentWritable(metaDocument);
			NodeStatus node = ListsManager.nextDatanode();
			ITextIndexerProtocol proxy = proxyMap.get(node.getHostname());
			result = proxy.addText(documentWrap, new Text(content));			
			
			LOG.info("Indexação de "+metaDocument.getId()+" em "+node.getHostname()
						+" retornou "+ReturnMessage.getReturnMessage(result.get()));
			
			return ReturnMessage.getReturnMessage(result.get());
		
		} catch (ConcurrentModificationException e) {
			LOG.info("Reordenando listas.");
		} catch (Throwable e){
			e.getStackTrace();
		}
		
		return ReturnMessage.ID_NOT_FOUND;
	}

	@Override
	public ReturnMessage delText(final String identifier) {
		ReturnMessage message = ReturnMessage.ID_NOT_FOUND;
		
		loadProxy();
		
		try {
			ArrayList<Future<IntWritable>> requests = new ArrayList<Future<IntWritable>>();
			for (final NodeStatus nodeStatus : datanodes) {
				Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
					@Override
					public IntWritable call() throws Exception {
						ITextIndexerProtocol proxy = proxyMap.get(nodeStatus.getHostname());
						return proxy.delText(new Text(identifier));
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
		
		} catch (ConcurrentModificationException e) {
			LOG.info("Reordenando listas.");
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (ExecutionException e) {
			LOG.info("Reordenando listas.");
		} catch (TimeoutException e) {
			LOG.error(e);
		} catch (Throwable e){
			LOG.error(e);
		}
		return message;
	}

	@Override
	public ReturnMessage updateText(final String id, final Map<String, String> metaDocument) {
		ReturnMessage message = ReturnMessage.ID_NOT_FOUND;
		
		loadProxy();
		
		try {
			ArrayList<Future<IntWritable>> requests = new ArrayList<Future<IntWritable>>();
			for (final NodeStatus nodeStatus : datanodes) {
				Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
					@Override
					public IntWritable call() throws Exception {
						ITextIndexerProtocol proxy = proxyMap.get(nodeStatus.getHostname());
						return proxy.updateText(new Text(id), WritableUtils.convertMapToMapWritable(metaDocument));
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
		
		} catch (ConcurrentModificationException e) {
			LOG.info("Reordenando listas.");
		} catch (InterruptedException e) {
			LOG.error(e);
		} catch (ExecutionException e) {
			LOG.info("Reordenando listas.");
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

}
