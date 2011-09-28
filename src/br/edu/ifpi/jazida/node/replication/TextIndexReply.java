package br.edu.ifpi.jazida.node.replication;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.PathJazida;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.ReturnMessage;
import br.edu.ifpi.opala.utils.Util;

public class TextIndexReply {

	public static final Analyzer ANALYZER = new BrazilianAnalyzer(Version.LUCENE_30);
	private static final Logger LOG = Logger.getLogger(TextIndexReply.class);
	private String pathDir = PathJazida.TEXT_INDEX_REPLY.getValue();
	private String HOSTNAME_LOCAL = DataNodeConf.DATANODE_HOSTNAME;
	private static TextIndexReply textIndexReply = new TextIndexReply();
	
	private TextIndexReply() {
	}

	public static TextIndexReply getTextIndexUtil() {
		if (textIndexReply == null) {
			textIndexReply = new TextIndexReply();
		}
		return textIndexReply;
	}
	
	public synchronized ReturnMessage addTextReply(MetaDocument metaDocument, String content,
			String hostname, String IP, long numDocsIndex) throws IOException {
		metaDocument.getDocument().add(
				new Field(Metadata.CONTENT.getValue(), content,
						Field.Store.YES, Field.Index.ANALYZED));
		
		try {		
			
			IndexWriter writer = getIndexWriter(hostname);
			writer.addDocument(metaDocument.getDocument());
			System.out.println("add reply");
			long numDocsReply = writer.numDocs();			
			writer.close();			
		
			if (numDocsIndex > numDocsReply){
				LOG.info("Atualizando réplica do "+ hostname + ".");
				//getDiretory(hostname).deleteFile("0_.cfs");
				new SupportReplyText().startUpdateIndexReply(IP, getDiretory(hostname), HOSTNAME_LOCAL);
				System.out.println("replica atualizada: metodo 1");
				return ReturnMessage.OUTDATED;
			}
			
			
		} catch (CorruptIndexException e) {
			LOG.info("Restaurando réplica de texto do "+ hostname + ".");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyText().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} catch (IOException e) {
			LOG.info("Restaurando réplica de texto do "+ hostname + ".");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyText().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		}
		
		return ReturnMessage.SUCCESS;
	}

	public synchronized ReturnMessage delTextReply(String id, String hostname, String IP) throws IOException {
		
		try {
			IndexWriter writer = getIndexWriter(hostname);
			writer.deleteDocuments(new Term(Metadata.ID.getValue(), id));
			System.out.println("Deletou reply");
			writer.optimize();
			writer.close();			
			return ReturnMessage.SUCCESS;
		} catch (CorruptIndexException e) {
			LOG.info("Restaurando réplica de texto do "+ hostname + ".");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyText().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} catch (IOException e) {
			LOG.info("Restaurando réplica de texto do "+ hostname + ".");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyText().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		}
	}
	
	public synchronized ReturnMessage updateTextReply(String id, Map<String, String> updates, String hostname, String IP) throws IOException {
		Term term = new Term(Metadata.ID.getValue(), id);
		try {
			Document doc = getDocumentByIdentifier(id, hostname);
			if (doc == null) {
				return ReturnMessage.ID_NOT_FOUND;
			}

			for (Map.Entry<String, String> entry : updates.entrySet()) {
				doc.removeField(entry.getKey());
				doc.add(new Field(entry.getKey(), entry.getValue(),
						Field.Store.YES, Field.Index.ANALYZED));
			}

			IndexWriter writer = getIndexWriter(hostname);
			writer.updateDocument(term, doc);
			writer.optimize();
			System.out.println("Atualizou reply");
			writer.close();
			return ReturnMessage.SUCCESS;
			
		} catch (CorruptIndexException e) {
			LOG.info("Restaurando réplica de texto do "+ hostname + ".");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyText().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} catch (IOException e) {
			LOG.info("Restaurando réplica de texto do "+ hostname + ".");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyText().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		}
		
	}
	
	private IndexWriter getIndexWriter(String hostname) throws CorruptIndexException, LockObtainFailedException, IOException{
		return new IndexWriter(getDiretory(hostname), ANALYZER,	MaxFieldLength.UNLIMITED);
	}

	private Directory getDiretory(String hostname) throws IOException {
		Directory dir = FSDirectory.open(new File(pathDir + "/" + hostname));
		return dir;
	}
	
	private Document getDocumentByIdentifier(String id, String hostname) throws IOException {
		IndexReader indexReader = null;
		Directory dir = null;
		try {
			dir = getDiretory(hostname);
			indexReader = IndexReader.open(dir, false);
			
			Term term = new Term(Metadata.ID.getValue(), id);
			TermDocs termDocs = indexReader.termDocs(term);
			while (termDocs.next()) {
				int docUID = termDocs.doc();
				if (!indexReader.isDeleted(docUID)) {
					return indexReader.document(docUID);
				}
			}
			
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		indexReader.close();
		return null;
	}
}
