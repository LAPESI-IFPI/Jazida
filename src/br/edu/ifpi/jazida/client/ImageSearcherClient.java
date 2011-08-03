package br.edu.ifpi.jazida.client;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.exception.NoNodesAvailableException;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.protocol.IImageSearchProtocol;
import br.edu.ifpi.jazida.util.ListsManager;
import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.SearchResultWritable;
import br.edu.ifpi.opala.searching.ResultItem;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.searching.SearcherImage;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class ImageSearcherClient implements SearcherImage {
	
	private static final Logger LOG = Logger.getLogger(ImageIndexerClient.class);
	private static final Configuration HADOOP_CONF = new Configuration();
	private List<NodeStatus> datanodes;
	private Map<String, IImageSearchProtocol> proxyMap = new HashMap<String, IImageSearchProtocol>();
	private ExecutorService threadPool;

	public ImageSearcherClient() throws IOException, KeeperException, InterruptedException {
		LOG.info("Inicializando ImageSearchClient");
		threadPool = Executors.newCachedThreadPool();
	}

	private IImageSearchProtocol getImageSearchProtocolProxy(InetSocketAddress hostAdress){
	 	try{	
			IImageSearchProtocol proxy = (IImageSearchProtocol) RPC.getProxy(
		 														IImageSearchProtocol.class,
		 														IImageSearchProtocol.versionID,
																hostAdress, HADOOP_CONF);
			
			return proxy;
	 	} catch (IOException e){
	 		LOG.info(e);
	 	}
		return null;
	}
	
	private void loadProxy() {
		datanodes = ListsManager.getDataNodes();
		if (datanodes.size() == 0)
			throw new NoNodesAvailableException("Nenhum DataNode conectado ao ClusterService.");
		
		for (NodeStatus node : datanodes) {
			final InetSocketAddress hostAdress = new InetSocketAddress(
														node.getAddress(),
														node.getImageSearcherServerPort());
			
			IImageSearchProtocol imageSearchProtocolProxy = getImageSearchProtocolProxy(hostAdress);
			if(imageSearchProtocolProxy != null)
			proxyMap.put(node.getHostname(), imageSearchProtocolProxy);
		}
	}

	@Override
	public SearchResult search(final BufferedImage image, final int limit) {
		
		loadProxy();
		
		if (image == null) {
			return new SearchResult(ReturnMessage.PARAMETER_INVALID, null);
		}
		
		List<Future<SearchResultWritable>> requests = new ArrayList<Future<SearchResultWritable>>();
		try {
			for (final NodeStatus nodeStatus : datanodes) {
				Future<SearchResultWritable> request = threadPool.submit(new Callable<SearchResultWritable>() {
					@Override
					public SearchResultWritable call() throws Exception {
						IImageSearchProtocol proxy = proxyMap.get(nodeStatus.getHostname());
						return proxy.search(new BufferedImageWritable(image), new IntWritable(limit));
					}
				});
				requests.add(request);
			}
			
			List<SearchResult> results = new ArrayList<SearchResult>();
			for (Future<SearchResultWritable> future : requests) {
				
				SearchResultWritable result = future.get(3000, TimeUnit.MILLISECONDS);
				if(result.isError())
					throw new RemoteException("Ocorreu uma exeção no DataNode.", result.getException());
				
				results.add(result.getSearchResult());
			}
			
			return sortAndFilterResults(results, limit);
			
		} catch (InterruptedException e) {
			LOG.error(e);
			return new SearchResult(ReturnMessage.UNEXPECTED_SEARCH_ERROR, null);
		} catch (ExecutionException e) {
			LOG.error(e);
			return new SearchResult(ReturnMessage.UNEXPECTED_SEARCH_ERROR, null);
		} catch (TimeoutException e) {
			LOG.error(e);
			return new SearchResult(ReturnMessage.UNEXPECTED_SEARCH_ERROR, null);
		} catch (IOException e) {
			LOG.error(e);
			return new SearchResult(ReturnMessage.UNEXPECTED_SEARCH_ERROR, null);
		}  catch (Throwable e){
			LOG.error(e);
			return new SearchResult(ReturnMessage.UNEXPECTED_SEARCH_ERROR, null);
		}
		
	}
	
	class SearchResultComparator implements Comparator<ResultItem>  {
		@Override
		public int compare(ResultItem o1, ResultItem o2) {
			return (int) (Double.parseDouble(o1.getScore()) - Double.parseDouble(o2.getScore()));
		}
	}

	private SearchResult sortAndFilterResults(List<SearchResult> results, int limit) {
		ReturnMessage message = ReturnMessage.SUCCESS; 
		ArrayList<ResultItem> all = new ArrayList<ResultItem>();
		for (SearchResult searchResult : results) {
			all.addAll(searchResult.getItems());
			
			if(searchResult.getCodigo() != ReturnMessage.SUCCESS)
				message = searchResult.getCodigo();
		}
		Collections.sort(all, new SearchResultComparator());
		
		List<ResultItem> finalList = new ArrayList<ResultItem>();
		limit = Math.min(limit, all.size());
		for(int i = 0; i < limit; i++) {
			finalList.add(all.get(i));
		}
		return new SearchResult(message, finalList);
	}

	@Override
	public SearchResult search(Map<String, String> fields,
			List<String> returnedFields, int batchStart, int batchSize,
			String sortOn, boolean reverse) {
		
		
		
		return null;
	}

	@Override
	public SearchResult search(Map<String, String> fields,
			List<String> returnedFields, int batchStart, int batchSize,
			String sortOn) {
		// TODO Auto-generated method stub
		return null;
	}

}
