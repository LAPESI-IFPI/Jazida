package br.edu.ifpi.jazida.node.protocol;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ParallelMultiSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.util.PathJazida;
import br.edu.ifpi.jazida.writable.CollectorWritable;
import br.edu.ifpi.jazida.writable.DocumentWritable;
import br.edu.ifpi.jazida.writable.ExplanationWritable;
import br.edu.ifpi.jazida.writable.FieldSelectorWritable;
import br.edu.ifpi.jazida.writable.FilterWritable;
import br.edu.ifpi.jazida.writable.QueryWritable;
import br.edu.ifpi.jazida.writable.SortWritable;
import br.edu.ifpi.jazida.writable.TermWritable;
import br.edu.ifpi.jazida.writable.TopDocsWritable;
import br.edu.ifpi.jazida.writable.TopFieldDocsWritable;
import br.edu.ifpi.jazida.writable.WeightWritable;
import br.edu.ifpi.opala.utils.IndexManager;

public class TextSearchableProtocol implements ITextSearchableProtocol {
	
	private static final Logger LOG = Logger.getLogger(TextSearchableProtocol.class);
	private IndexManager indexManager;
	private IndexSearcher[] searchers = new IndexSearcher[2];
	private ParallelMultiSearcher multiSearcher;
	private NodeStatus node;

	public TextSearchableProtocol(IndexManager manager, NodeStatus node) {
		super();
		this.indexManager = manager;
		this.node = node;
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		if(node.isTwoResponding())
			createMultiSeacher();
		return 0;
	}
	
	
	@Override
	public void close(){
		try {
			if(node.isTwoResponding()){
				multiSearcher.close();
			} else{
				IndexSearcher searcher = indexManager.getSearcher();
				searcher.close();
			}
		}catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.close()");
			LOG.error(e);
		}
	}
	
	@Override
	public DocumentWritable doc(IntWritable arg0) {
		try {
			IndexSearcher searcher = indexManager.getSearcher();
			try {
				if(node.isTwoResponding()){
					return new DocumentWritable(multiSearcher.doc(arg0.get()));
				} else{	
					return new DocumentWritable(searcher.doc(arg0.get()));
				}
			}finally {
				indexManager.release(searcher);
			}
		} catch (CorruptIndexException e) {
			LOG.error("Falha em TextSearchableProtocol.doc(IntWritable)");
			LOG.error(e);
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.doc(IntWritable)");
			LOG.error(e);
		}
		return null;
	}

	@Override
	public DocumentWritable doc(IntWritable arg0, FieldSelectorWritable arg1) {
		try {
			IndexSearcher searcher = indexManager.getSearcher();
			try {
				if(node.isTwoResponding()){	
					Document doc = multiSearcher.doc(arg0.get(), arg1.getFieldSelector());
					return new DocumentWritable(doc);
				} else{		
					Document doc = searcher.doc(arg0.get(), arg1.getFieldSelector());
					return new DocumentWritable(doc);
				}
			}finally {
				indexManager.release(searcher);
			}
		} catch (Exception e) {
			LOG.error("Falha em TextSearchableProtocol.doc(IntWritable, FieldSelector)");
			LOG.error(e);
		};
		return null;
	}

	@Override
	public IntWritable docFreq(TermWritable arg0) {
		try {
			IndexSearcher searcher = indexManager.getSearcher();
			try {
				if(node.isTwoResponding()){
					return new IntWritable(multiSearcher.docFreq(arg0.getTerm()));
				} else{	
					return new IntWritable(searcher.docFreq(arg0.getTerm()));
				}
			}finally {
				indexManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.docFreqs(TermWritable[])");
			LOG.error(e);
		}
		return null;
	}

	@Override
	public IntWritable[] docFreqs(TermWritable[] termsWritable) {
		try {
			if(node.isTwoResponding()){
				Term[] terms = new Term[termsWritable.length]; 
				for (int i = 0; i < termsWritable.length; i++) {
					terms[i] = termsWritable[i].getTerm();
				}
	
				int[] docFreqs;
				IndexSearcher searcher = indexManager.getSearcher();
				try {
					docFreqs = multiSearcher.docFreqs(terms);
				}finally {
					indexManager.release(searcher);
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
				IndexSearcher searcher = indexManager.getSearcher();
				try {
					docFreqs = searcher.docFreqs(terms);
				}finally {
					indexManager.release(searcher);
				}
				
				IntWritable[] freqs = new IntWritable[docFreqs.length];
				for (int i = 0; i < docFreqs.length; i++) {
					freqs[i] = new IntWritable(docFreqs[i]);
				}
				
				return freqs;
			}
		} catch (IOException e) {
			LOG.error("Falha TextSearchableProtocol.docFreqs(TermWritable[])");
			LOG.error(e);
			return null;
		}
	}

	@Override
	public ExplanationWritable explain(WeightWritable arg0, IntWritable arg1) {
		try {
			IndexSearcher searcher = indexManager.getSearcher();
			try {
				if(node.isTwoResponding()){	
					return new ExplanationWritable(multiSearcher.explain(arg0.getWeight(), arg1.get()));
				} else{	
					return new ExplanationWritable(searcher.explain(arg0.getWeight(), arg1.get()));
				}
			}finally {
				indexManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha TextSearchableProtocol.explain()");
			LOG.error(e);
			return null;
		}
	}

	@Override
	public IntWritable maxDoc() {
		try {
			IndexSearcher searcher = indexManager.getSearcher();
			try {
				if(node.isTwoResponding()){
					return new IntWritable(multiSearcher.maxDoc());
				} else{	
					return new IntWritable(searcher.maxDoc());
				}
			}finally {
				indexManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.maxDoc()");
			LOG.error(e);
			return null;
		}
	}

	@Override
	public QueryWritable rewrite(QueryWritable arg0) {
		try {
			IndexSearcher searcher = indexManager.getSearcher();
			try {
				if(node.isTwoResponding()){	
					return new QueryWritable(multiSearcher.rewrite(arg0.getQuery()));
				} else{	
					return new QueryWritable(searcher.rewrite(arg0.getQuery()));
				}
			}finally {
				indexManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.rewrite(QueryWritable)");
			LOG.error(e);
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
			IndexSearcher searcher = indexManager.getSearcher();
			try {
				if(node.isTwoResponding()){		
					TopDocs search = multiSearcher.search(arg0.getWeight(), arg1.getFilter(), arg2.get());
					return new TopDocsWritable(search);
				} else{	
					TopDocs search = searcher.search(arg0.getWeight(), arg1.getFilter(), arg2.get());
					return new TopDocsWritable(search);
				}
			}finally {
				indexManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.search()");
			LOG.error(e);
			return null;
		}
	}

	@Override
	public TopFieldDocsWritable search(	WeightWritable arg0,
										FilterWritable arg1,
										IntWritable arg2,
										SortWritable arg3) {
		try {
			IndexSearcher searcher = indexManager.getSearcher();
			try {
				
				if(node.isTwoResponding()){				
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
				indexManager.release(searcher);
			}
		} catch (IOException e) {
			LOG.error("Falha em TextSearchableProtocol.search(WeightWritable,FilterWritable,IntWritable,SortWritabl)");
			LOG.error(e);
			return null;
		}
	}
	
	private synchronized void createMultiSeacher(){
		try {
			searchers[0] = new IndexSearcher(indexManager.getDirectory());
			searchers[1] = new IndexSearcher(getDiretory(node.getHostNameResponding()));
			multiSearcher = new ParallelMultiSearcher(searchers);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private Directory getDiretory(String hostname) throws IOException {
		String pathDir = PathJazida.TEXT_INDEX_REPLY.getValue();
		Directory dir = FSDirectory.open(new File(pathDir + "/" + hostname));
		return dir;
	}

}
