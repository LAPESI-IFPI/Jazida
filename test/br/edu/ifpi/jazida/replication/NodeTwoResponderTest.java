package br.edu.ifpi.jazida.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.BeforeClass;
import org.junit.Test;

import br.edu.ifpi.jazida.util.FileUtilsForTest;
import br.edu.ifpi.jazida.util.PathJazida;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.Path;

public class NodeTwoResponderTest {
	private static File fileIndex = new File(Path.TEXT_INDEX.getValue());
	private static File fileReply = new File(PathJazida.TEXT_INDEX_REPLY.getValue());
	private static final Analyzer ANALYZER = new BrazilianAnalyzer(Version.LUCENE_30);
	private static IndexSearcher[] searchers = new IndexSearcher[2];
	private static MultiSearcher multiSearcher;
	private static IndexWriter writerIndex, writerReply;
	private static Directory dirReply, dirIndex;
	private static MetaDocument docIndex = new MetaDocument();
	private static MetaDocument docReply = new MetaDocument();
	private static MetaDocument docReply2 = new MetaDocument();

	@BeforeClass
	public static void setUpTest() throws Exception {

		assertTrue(FileUtilsForTest.deleteDir(fileIndex));
		assertTrue(FileUtilsForTest.deleteDir(fileReply));
		createIndexes();
		indexerIndexes();
		
		searchers[0] = new IndexSearcher(dirIndex);
		searchers[1] = new IndexSearcher(dirReply);
		multiSearcher = new MultiSearcher(searchers);
	}
	
	@Test
	public void deveriaBuscarEmDoisIndicesDistintos() throws IOException {
		Query query = new TermQuery(new Term(Metadata.ID.getValue(), "doc2"));				
		int maxDoc = multiSearcher.maxDoc();
		Document doc = multiSearcher.doc(1);
		TopDocs topDocs = multiSearcher.search(query, 1);
		int freq = multiSearcher.docFreq(new Term(Metadata.TITLE.getValue(), "3"));
	
		assertEquals(3, maxDoc);
		assertEquals(docReply.getDocument().get(Metadata.CONTENT.getValue()), doc.get(Metadata.CONTENT.getValue()));
		assertEquals(1, topDocs.totalHits); 
		assertEquals(1, freq);
		
	}
	
	private static void createIndexes() throws IOException{
		dirIndex = FSDirectory.open(fileIndex);
		dirReply = FSDirectory.open(fileReply);
		
		writerIndex = new IndexWriter(dirIndex, ANALYZER, IndexWriter.MaxFieldLength.UNLIMITED);
		
		writerReply = new IndexWriter(dirReply, ANALYZER, IndexWriter.MaxFieldLength.UNLIMITED);
	}
	
	private static void indexerIndexes() throws CorruptIndexException, IOException{
		docIndex.setTitle("Documento de Teste");
		docIndex.setId("doc1");
		
		docReply.setTitle("Documento de Teste 2");
		docReply.setId("doc2");
		
		docReply2.setTitle("Documento de Teste 3");
		docReply2.setId("doc3");

		docIndex.getDocument().add(
				new Field(Metadata.CONTENT.getValue(), "Esse é um documento a ser utilizado para o teste de busca em dois indices" +
						" sendo que este será indexado no indice original",	Field.Store.YES, Field.Index.ANALYZED));
		
		docReply.getDocument().add(
				new Field(Metadata.CONTENT.getValue(), "replica", Field.Store.YES, Field.Index.ANALYZED));
		
		docReply2.getDocument().add(
				new Field(Metadata.CONTENT.getValue(), "Esse é um documento a ser utilizado para o teste de busca em dois indices" +
						" sendo que este tambem será indexado no indice da replica", Field.Store.YES, Field.Index.ANALYZED));
		
		
		writerIndex.addDocument(docIndex.getDocument());
		writerReply.addDocument(docReply.getDocument());
		writerReply.addDocument(docReply2.getDocument());
				
		writerIndex.close();
		writerReply.close();
	}
}
