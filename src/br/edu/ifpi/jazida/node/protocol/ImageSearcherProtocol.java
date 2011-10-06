package br.edu.ifpi.jazida.node.protocol;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.ImageSearchHits;
import net.semanticmetadata.lire.ImageSearcher;
import net.semanticmetadata.lire.ImageSearcherFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ParallelMultiSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.util.PathJazida;
import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.CollectorWritable;
import br.edu.ifpi.jazida.writable.DocumentWritable;
import br.edu.ifpi.jazida.writable.ExplanationWritable;
import br.edu.ifpi.jazida.writable.FieldSelectorWritable;
import br.edu.ifpi.jazida.writable.FilterWritable;
import br.edu.ifpi.jazida.writable.QueryWritable;
import br.edu.ifpi.jazida.writable.SearchResultWritable;
import br.edu.ifpi.jazida.writable.SortWritable;
import br.edu.ifpi.jazida.writable.TermWritable;
import br.edu.ifpi.jazida.writable.TopDocsWritable;
import br.edu.ifpi.jazida.writable.TopFieldDocsWritable;
import br.edu.ifpi.jazida.writable.WeightWritable;
import br.edu.ifpi.opala.searching.ResultItem;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.searching.SearcherImage;
import br.edu.ifpi.opala.searching.SearcherImageImpl;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.ReturnMessage;
import br.edu.ifpi.opala.utils.SearcherManager;

public class ImageSearcherProtocol implements IImageSearchProtocol {
	private static final Logger LOG = Logger.getLogger(ImageSearcherProtocol.class);
	SearcherManager seacherManager;
	Directory directory;
	private IndexSearcher[] searchers;
	private ParallelMultiSearcher multiSearcher;
	private NodeStatus node;
	private int qtdResponding;
	
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		int qtd = node.getNodesResponding().size();
		if(qtdResponding != qtd){
			qtdResponding = qtd;
			if(qtdResponding > 0){
				int cont = 0;
				for(String hostName: node.getNodesResponding()){
					if(new File(PathJazida.IMAGE_INDEX_REPLY.getValue() + "/" + hostName).canRead()){
						cont++;
					}
				}
				searchers = new IndexSearcher[cont + 1];
				createMultiSeacher();
			}
		}
		return 0;
	}
	
	public ImageSearcherProtocol(Directory directory, NodeStatus node) throws IOException {
		super();
		seacherManager = new SearcherManager(directory);
		this.directory = directory;
		this.node = node;		
	}
	
	@Override
	public SearchResultWritable search(	BufferedImageWritable image,
										IntWritable limit) {
		try {
			if(qtdResponding > 0){
				SearchResult searcherResult = multiSearch(node, image.getBufferedImage(), limit.get());
				return new SearchResultWritable(searcherResult);
				
			}else{
				SearcherImage searcher = new SearcherImageImpl();
				return new SearchResultWritable(searcher.search(image.getBufferedImage(), limit.get()));
			}
		} catch (Exception e) {
			return new SearchResultWritable(e);
		}
	}
	
	private SearchResult multiSearch(NodeStatus node, BufferedImage image, int limit) {
		ImageSearchHits hits = null;
		Directory dir = null;
		IndexReader reader = null;
		List<ResultItem> items = new ArrayList<ResultItem>();
		List<String> listSearcher = new ArrayList<String>();
		
		listSearcher.add(node.getHostname());
		listSearcher.addAll(node.getNodesResponding());
		
		if (limit <= 0) {
			return new SearchResult(ReturnMessage.PARAMETER_INVALID, null);
		}
		
		try {
			for(String hostName: listSearcher){
				if(hostName.equals(node.getHostname())){
					dir = FSDirectory.open(new File(Path.IMAGE_INDEX.getValue()));
				}
				else{
					dir = getDiretory(hostName);
				}
				if (IndexReader.indexExists(dir)) {
					reader = IndexReader.open(dir, true);
		
					if (reader.numDocs() == 0) {
						reader.close();
						return new SearchResult(ReturnMessage.EMPTY_INDEX, null);
					}
					ImageSearcher searcher = ImageSearcherFactory.createCEDDImageSearcher(limit);
					hits = searcher.search(image, reader);
					
				} else {
					return new SearchResult(ReturnMessage.INDEX_NOT_FOUND, null);
				}
	
				for (int i = 0; i < hits.length(); i++) {
					ResultItem item = new ResultItem();
					item.setId(hits.doc(i).getField(DocumentBuilder.FIELD_NAME_IDENTIFIER).stringValue());
					item.setScore(String.valueOf(hits.score(i)));
					
					if (i > 0 && item.getScore().equals(items.get(i - 1).getScore()))
						item.setDuplicated(true);
					else
						item.setDuplicated(false);
					
					items.add(item);
				}
				
				reader.close();
			}
			
			if (items.size() != 0) {
				return new  SearchResult(ReturnMessage.SUCCESS, items);
			} else {
				return new  SearchResult(ReturnMessage.EMPTY_SEARCHER, items);
			}
			
		} catch (IOException e) {
			return new SearchResult(ReturnMessage.UNEXPECTED_ERROR, null);
		}
	}

	@Override
	public void close(){
		try {
			if(qtdResponding > 0){
				multiSearcher.close();
			} else{
				IndexSearcher searcher = getSearcher();
				searcher.close();
			}
		}catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.close()");
			LOG.error(e.fillInStackTrace(), e);
		}
	}
	
	@Override
	public DocumentWritable doc(IntWritable arg0) {
		try {
			IndexSearcher searcher = getSearcher();
			try {
				if(qtdResponding > 0){
					return new DocumentWritable(multiSearcher.doc(arg0.get()));
				} else{	
					return new DocumentWritable(searcher.doc(arg0.get()));
				}
			}finally {
				seacherManager.release(searcher);
			}
		} catch (CorruptIndexException e) {
			LOG.error("Falha em TextSearchableProtocol.doc(IntWritable)");
			LOG.error(e.fillInStackTrace(), e);
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.doc(IntWritable)");
			LOG.error(e.fillInStackTrace(), e);
		}
		return null;
	}

	@Override
	public DocumentWritable doc(IntWritable arg0, FieldSelectorWritable arg1) {
		try {
			IndexSearcher searcher = getSearcher();
			try {
				if(qtdResponding > 0){	
					Document doc = multiSearcher.doc(arg0.get(), arg1.getFieldSelector());
					return new DocumentWritable(doc);
				} else{		
					Document doc = searcher.doc(arg0.get(), arg1.getFieldSelector());
					return new DocumentWritable(doc);
				}
			}finally {
				seacherManager.release(searcher);
			}
		} catch (Exception e) {
			LOG.error("Falha em TextSearchableProtocol.doc(IntWritable, FieldSelector)");
			LOG.error(e.fillInStackTrace(), e);
		};
		return null;
	}

	@Override
	public IntWritable docFreq(TermWritable arg0) {
		try {
			IndexSearcher searcher = getSearcher();
			try {
				if(qtdResponding > 0){
					return new IntWritable(multiSearcher.docFreq(arg0.getTerm()));
				} else{	
					return new IntWritable(searcher.docFreq(arg0.getTerm()));
				}
			}finally {
				seacherManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.docFreqs(TermWritable[])");
			LOG.error(e.fillInStackTrace(), e);
		}
		return null;
	}

	@Override
	public IntWritable[] docFreqs(TermWritable[] termsWritable) {
		try {
			if(qtdResponding > 0){
				Term[] terms = new Term[termsWritable.length]; 
				for (int i = 0; i < termsWritable.length; i++) {
					terms[i] = termsWritable[i].getTerm();
				}
	
				int[] docFreqs;
				IndexSearcher searcher = getSearcher();
				try {
					docFreqs = multiSearcher.docFreqs(terms);
				}finally {
					seacherManager.release(searcher);
				}
				
				IntWritable[] freqs = new IntWritable[docFreqs.length];
				for (int i = 0; i < docFreqs.length; i++) {
					freqs[i] = new IntWritable(docFreqs[i]);
				}
				
				return freqs;
				
			} else{	
				
				Term[] terms = new Term[termsWritable.length]; 
				for (int i = 0; i < termsWritable.length; i++) {
					terms[i] = termsWritable[i].getTerm();
				}
	
				int[] docFreqs;
				IndexSearcher searcher = getSearcher();
				try {
					docFreqs = searcher.docFreqs(terms);
				}finally {
					seacherManager.release(searcher);
				}
				
				IntWritable[] freqs = new IntWritable[docFreqs.length];
				for (int i = 0; i < docFreqs.length; i++) {
					freqs[i] = new IntWritable(docFreqs[i]);
				}
				
				return freqs;
			}
		} catch (IOException e) {
			LOG.error("Falha TextSearchableProtocol.docFreqs(TermWritable[])");
			LOG.error(e.fillInStackTrace(), e);
			return null;
		}
	}

	@Override
	public ExplanationWritable explain(WeightWritable arg0, IntWritable arg1) {
		try {
			IndexSearcher searcher = getSearcher();
			try {
				if(qtdResponding > 0){
					return new ExplanationWritable(multiSearcher.explain(arg0.getWeight(), arg1.get()));
				} else{	
					return new ExplanationWritable(searcher.explain(arg0.getWeight(), arg1.get()));
				}
			}finally {
				seacherManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha TextSearchableProtocol.explain()");
			LOG.error(e.fillInStackTrace(), e);
			return null;
		}
	}

	@Override
	public IntWritable maxDoc() {
		try {
			IndexSearcher searcher = getSearcher();
			try {
				if(qtdResponding > 0){
					return new IntWritable(multiSearcher.maxDoc());
				} else{	
					return new IntWritable(searcher.maxDoc());
				}
			}finally {
				seacherManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.maxDoc()");
			LOG.error(e.fillInStackTrace(), e);
			return null;
		}
	}

	@Override
	public QueryWritable rewrite(QueryWritable arg0) {
		try {
			IndexSearcher searcher = getSearcher();
			try {
				if(qtdResponding > 0){
					return new QueryWritable(multiSearcher.rewrite(arg0.getQuery()));
				} else{	
					return new QueryWritable(searcher.rewrite(arg0.getQuery()));
				}
			}finally {
				seacherManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.rewrite(QueryWritable)");
			LOG.error(e.fillInStackTrace(), e);
			return null;
		}
	}

	@Override
	public void search(WeightWritable arg0, FilterWritable arg1, CollectorWritable arg2) {
		//
		// TODO: Implementar search() com Collectors
		//
		String message = "search(WeightWritable, FilterWritable, CollectorWritable) NÃO IMPLEMENTADO!";
		LOG.error(message);
		throw new java.lang.UnsupportedOperationException(message);
	}

	@Override
	public TopDocsWritable search(WeightWritable arg0, FilterWritable arg1,
			IntWritable arg2) {
		try {
			IndexSearcher searcher = getSearcher();
			try {
				if(qtdResponding > 0){
					TopDocs search = multiSearcher.search(arg0.getWeight(), arg1.getFilter(), arg2.get());
					return new TopDocsWritable(search);
				} else{	
					TopDocs search = searcher.search(arg0.getWeight(), arg1.getFilter(), arg2.get());
					return new TopDocsWritable(search);
				}
			}finally {
				seacherManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.search()");
			LOG.error(e.fillInStackTrace(), e);
			return null;
		}
	}

	@Override
	public TopFieldDocsWritable search(	WeightWritable arg0,
										FilterWritable arg1,
										IntWritable arg2,
										SortWritable arg3) {
		try {
			IndexSearcher searcher = getSearcher();
			try {
				
				if(qtdResponding > 0){		
					TopFieldDocs topdocs = multiSearcher.search(arg0.getWeight(),
							arg1.getFilter(),
							arg2.get(),
							arg3.getSort());
					return new TopFieldDocsWritable(topdocs);
				} else {	
					TopFieldDocs topdocs = searcher.search(	arg0.getWeight(),
							arg1.getFilter(),
							arg2.get(),
							arg3.getSort());
					return new TopFieldDocsWritable(topdocs);
				}
			}finally {
				seacherManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.search(WeightWritable,FilterWritable,IntWritable,SortWritabl)");
			LOG.error(e.fillInStackTrace(), e);
			return null;
		}
	}
	
	
	private synchronized void createMultiSeacher(){
		try {
			int i = 0;
			searchers[i] = new IndexSearcher(directory);
			for (String hostName: node.getNodesResponding()){
				i++;
				searchers[i] = new IndexSearcher(getDiretory(hostName));
			}
			multiSearcher = new ParallelMultiSearcher(searchers);
			System.out.println(multiSearcher.maxDoc());
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		catch (Throwable e) {
			e.printStackTrace();
		}
		
	}
		
	private Directory getDiretory(String hostName) throws IOException {
		String pathDir = PathJazida.IMAGE_INDEX_REPLY.getValue();
		Directory dir = FSDirectory.open(new File(pathDir + "/" + hostName));
		return dir;
	}
	
	private IndexSearcher getSearcher() throws IOException {
		refresh();
		return seacherManager.get();
	}
	
	private void refresh() throws IOException {
		try {
			seacherManager.maybeReopen();
		} catch (InterruptedException e) {
			throw new RuntimeException(
					"Erro ao tentar reabrir o índice com as ultimas indexações.",
					e);
		}
	}
}
