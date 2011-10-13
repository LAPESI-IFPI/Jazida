package br.edu.ifpi.jazida.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.ParallelMultiSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searchable;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Version;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.cluster.ClusterService;
import br.edu.ifpi.jazida.exception.NoNodesAvailableException;
import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.protocol.ITextSearchableProtocol;
import br.edu.ifpi.jazida.util.ListsManager;
import br.edu.ifpi.jazida.writable.FieldSelectorWritable;
import br.edu.ifpi.jazida.writable.FilterWritable;
import br.edu.ifpi.jazida.writable.QueryWritable;
import br.edu.ifpi.jazida.writable.SortWritable;
import br.edu.ifpi.jazida.writable.TermWritable;
import br.edu.ifpi.jazida.writable.WeightWritable;
import br.edu.ifpi.opala.searching.ResultItem;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.searching.TextSearcher;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class TextSearcherClient implements TextSearcher {
	
	private static final BrazilianAnalyzer ANALYZER = new BrazilianAnalyzer(Version.LUCENE_30);
	private static final Configuration HADOOP_CONF = new Configuration();
	private ParallelMultiSearcher searcher;
	private List<NodeStatus> datanodes;
	private Map<String, ITextSearchableProtocol> proxyMap = new HashMap<String, ITextSearchableProtocol>();
	
	public TextSearcherClient() 
	throws IOException, KeeperException, InterruptedException {
		this.datanodes = ListsManager.getDatanodes();

		if (datanodes.size()==0) {
			throw new NoNodesAvailableException("Nenhum DataNode conectado ao ClusterService.");
		}
		Searchable[] searchables = new RemoteSearchableAdapter[datanodes.size()];
		int i=0;
		for (NodeStatus node : datanodes) {
			final InetSocketAddress hostAdress = new InetSocketAddress(
							node.getAddress(),
							node.getTextSearchServerPort());
			ITextSearchableProtocol searchableClient = getSearchableProxy(hostAdress);
			proxyMap.put(node.getHostname(), searchableClient);
			searchables[i] = new RemoteSearchableAdapter(searchableClient);
			i++;
		}
		
		searcher = new ParallelMultiSearcher(searchables);
	}
	
	private ITextSearchableProtocol getSearchableProxy(
			final InetSocketAddress hostAdress) throws IOException {
		
		ITextSearchableProtocol proxy = (ITextSearchableProtocol) RPC.getProxy(
											ITextSearchableProtocol.class,
											ITextSearchableProtocol.versionID,
											hostAdress, HADOOP_CONF);
		return proxy;
	}

	@Override
	public SearchResult search(	Map<String, String> fields,
								List<String> returnedFields,
								int batchStart,
								int batchSize,
								String sortOn,
								boolean reverse) {
		
		if (fields == null || fields.size() == 0) {
			return new SearchResult(ReturnMessage.INVALID_QUERY, null);
		}
		
		try {
			int init = batchStart <= 0 ? 0 : batchStart - 1;
			int limit = batchSize <= 0 ? batchStart + 20 : batchStart + batchSize - 1;
			
			Sort sort = createSort(sortOn, reverse);
			Query query = createQuery(fields);
			
			ScoreDoc[] hits;
			if(sort == null) {
				hits = searcher.search(query, null, limit).scoreDocs;
			}else {
				hits = searcher.search(query, null, limit, sort).scoreDocs;
			}

			if (init >= hits.length) {
				return new SearchResult(ReturnMessage.EMPTY_SEARCHER, 
										new ArrayList<ResultItem>());
			}			
			
			return createResultItens(init, hits, returnedFields);
			
		} catch (ParseException e) {
			return new SearchResult(ReturnMessage.INVALID_QUERY, null);
		} catch (IOException e) {
			return new SearchResult(ReturnMessage.UNEXPECTED_INDEX_ERROR, null);
		} catch (Throwable e){
			return new SearchResult(ReturnMessage.UNEXPECTED_SEARCH_ERROR, null);
		}
	}

	@Override
	public SearchResult search( Map<String, String> fields,
								List<String> returnedFields,
								int batchStart,
								int batchSize,
								String sortOn) {
		
		return search(fields, returnedFields, batchStart, batchSize, sortOn, false);
	}
	

	private Sort createSort(String sortOn, boolean reverse) {
		Sort sort = null;
		if (sortOn != null && !sortOn.equals("")){
			SortField sf = new SortField(sortOn, SortField.STRING, reverse);
			sort = new Sort(sf);
		}
		return sort;
	}
	
	private Query createQuery(Map<String, String> fields) throws ParseException {
		QueryParser queryParser = new QueryParser(Version.LUCENE_30, null, ANALYZER);
		StringBuffer queryString = new StringBuffer();
		for (Map.Entry<String, String> entry : fields.entrySet()) {
			queryString.append(entry.getKey());
			queryString.append(":\"");
			queryString.append(entry.getValue());
			queryString.append("\" ");
		}
		return queryParser.parse(queryString.toString());
	}
	
	private SearchResult createResultItens( int init, 
											ScoreDoc[] hits,
											List<String> returnedFields) throws CorruptIndexException, IOException {
		
		ArrayList<ResultItem> items =  new ArrayList<ResultItem>();
		
		for (int i = init; i < hits.length; i++) {
			
			Document document = searcher.doc(hits[i].doc);

			ResultItem resultItem = new ResultItem();
			resultItem.setId(document.get(Metadata.ID.getValue()));
			resultItem.setScore(Float.toString(hits[i].score));
			
			if (i > 0 && hits[i].score == hits[i-1].score) {
				resultItem.setDuplicated(true);
			}

			Map<String, String> docFields = new HashMap<String, String>();
			if (returnedFields != null) {
				for (String field : returnedFields) {
					String fieldValue = document.get(field);
					if (fieldValue != null) {
						docFields.put(field, fieldValue);
					}
				}
			}
			resultItem.setFields(docFields);
			items.add(resultItem);
		}
		
		SearchResult result = new SearchResult(ReturnMessage.SUCCESS, items);
		return result;
	}
	
	public void close() throws InterruptedException {
		ClusterService.disconnect();
	}
	
	private class RemoteSearchableAdapter implements Searchable {
		
		ITextSearchableProtocol searchableProxy;
		
		public RemoteSearchableAdapter(ITextSearchableProtocol searchableProxy) {
			this.searchableProxy = searchableProxy;
		}

		@Override
		public void close() throws IOException {
			searchableProxy.close();
		}

		@Override
		public Document doc(int arg0) throws CorruptIndexException, IOException {
			return searchableProxy.doc(new IntWritable(arg0)).getDocument();
		}

		@Override
		public Document doc(int arg0, FieldSelector arg1)
				throws CorruptIndexException, IOException {
			return searchableProxy.doc(new IntWritable(arg0), new FieldSelectorWritable(arg1)).getDocument();
		}

		@Override
		public int docFreq(Term arg0) throws IOException {
			return searchableProxy.docFreq(new TermWritable(arg0)).get();
		}

		@Override
		public int[] docFreqs(Term[] terms) throws IOException {
			TermWritable[] termsWritable = new TermWritable[terms.length]; 
			for (int i = 0; i < terms.length; i++) {
				termsWritable[i] = new TermWritable(terms[i]);
			}
			
			IntWritable[] docFreqs = searchableProxy.docFreqs(termsWritable);
			
			int[] freqs = new int[docFreqs.length];
			for (int i = 0; i < docFreqs.length; i++) {
				freqs[i] = docFreqs[i].get();
			}
			return freqs;
		}

		@Override
		public Explanation explain(Weight arg0, int arg1) throws IOException {
			return searchableProxy.explain(new WeightWritable(arg0), new IntWritable(arg1)).getExplanation();
		}

		@Override
		public int maxDoc() throws IOException {
			return searchableProxy.maxDoc().get();
		}

		@Override
		public Query rewrite(Query arg0) throws IOException {
			return searchableProxy.rewrite(new QueryWritable(arg0)).getQuery();
		}

		@Override
		public void search(Weight arg0, Filter arg1, Collector arg2) throws IOException {
			//
			// TODO: implementar busca com Collectors
			//
			throw new UnsupportedOperationException("Operação ainda não implementada!");
		}

		@Override
		public TopDocs search(Weight arg0, Filter arg1, int arg2)
				throws IOException {
			return searchableProxy.search(new WeightWritable(arg0), new FilterWritable(arg1), new IntWritable(arg2)).getTopDocs();
		}

		@Override
		public TopFieldDocs search(Weight arg0, Filter arg1, int arg2, Sort arg3)
				throws IOException {
			return searchableProxy.search(new WeightWritable(arg0), new FilterWritable(arg1), new IntWritable(arg2), new SortWritable(arg3)).getTopFieldDocs();
		}
		
	}
}
