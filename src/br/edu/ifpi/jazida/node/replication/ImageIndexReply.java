package br.edu.ifpi.jazida.node.replication;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.semanticmetadata.lire.DocumentBuilder;
import net.semanticmetadata.lire.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
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

public class ImageIndexReply {
	public static final Analyzer ANALYZER = new BrazilianAnalyzer(Version.LUCENE_30);
	private static final Logger LOG = Logger.getLogger(ImageIndexReply.class);
	private String pathDir = PathJazida.IMAGE_INDEX_REPLY.getValue();
	private String HOSTNAME_LOCAL = DataNodeConf.DATANODE_HOSTNAME;
	private static int cont = 0;
	private static ImageIndexReply imageIndexReply = new ImageIndexReply();
	
	private ImageIndexReply() {
	}
	
	public static ImageIndexReply getTextIndexUtil() {
		if(imageIndexReply == null) {
			imageIndexReply = new ImageIndexReply();
		}
		return imageIndexReply;
	}


	public synchronized ReturnMessage addImageReply(MetaDocument metaDocument, BufferedImage image, String hostName,
			String IP, long numDocsIndex) throws IOException {
		
		DocumentBuilder builder = DocumentBuilderFactory.getCEDDDocumentBuilder();
		Document doc = builder.createDocument(image, metaDocument.getId());
		List<Fieldable> fields = doc.getFields();
		for (Fieldable field : fields) {
			metaDocument.getDocument().add(field);
		}

		try {			
			IndexWriter writer = getIndexWriter(hostName);			
			writer.addDocument(metaDocument.getDocument());
			long numDocsReply = writer.numDocs();
			writer.close();
			
			cont++;
			if(cont == 5){
				if (numDocsIndex != numDocsReply){
					LOG.info("Atualizando réplica de imagem do "+ hostName + "...");
					new SupportReplyImage().startUpdateIndexReply(IP, getDiretory(hostName), HOSTNAME_LOCAL);
					return ReturnMessage.OUTDATED;
				}
			cont = 0;
			}
			
		} catch (CorruptIndexException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostName + "...");
			Util.deleteDir(new File(pathDir+"/"+hostName));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} catch (IOException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostName + "...");
			Util.deleteDir(new File(pathDir+"/"+hostName));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} 

		return ReturnMessage.SUCCESS;
	}
	
	public synchronized ReturnMessage delImageReply(String id, String hostName, String IP) throws IOException {
		IndexReader reader;
		Directory dir = null;
		try {
			dir = getDiretory(hostName);
			reader = IndexReader.open(dir, false);

			for (int i = 0; i < reader.maxDoc(); i++) {
				if (!reader.isDeleted(i)) {
					if (reader.document(i)
							.getField(DocumentBuilder.FIELD_NAME_IDENTIFIER)
							.stringValue().equals(id)) {

						reader.deleteDocument(i);
						
					}
				}
			}		
			reader.close();
			dir.close();
			
			return ReturnMessage.SUCCESS;
		} catch (CorruptIndexException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostName + "...");
			Util.deleteDir(new File(pathDir+"/"+hostName));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} catch (IOException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostName + "...");
			Util.deleteDir(new File(pathDir+"/"+hostName));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} 
		
	}
	
	public synchronized ReturnMessage updateImageReply(String id,
			Map<String, String> updates, String hostName, String IP) throws IOException {
		try {
			Directory dir = getDiretory(hostName);
			IndexSearcher is = new IndexSearcher(dir, false);

				Query idQuery = new TermQuery(new Term(Metadata.ID.getValue(),
						id));
				TopDocs hits = is.search(idQuery, 5);
				if (hits.totalHits == 0
						|| !is.doc(hits.scoreDocs[0].doc)
								.get(Metadata.ID.getValue()).equals(id)) {
					is.close();
					dir.close();
					return ReturnMessage.ID_NOT_FOUND;
				}
				
				Document doc = is.doc(hits.scoreDocs[0].doc);
				for (Map.Entry<String, String> entry : updates.entrySet()) {
					doc.removeField(entry.getKey());
					doc.add(new Field(entry.getKey(), entry.getValue(),
							Field.Store.YES, Field.Index.ANALYZED));
				}
				is.close();
				
				if (!this.delImageReply(id, hostName, IP).equals(ReturnMessage.SUCCESS)) {
					return ReturnMessage.ID_NOT_FOUND;
				}

				IndexWriter writer = getIndexWriter(hostName);
				writer.addDocument(doc);
				writer.optimize();
				writer.close();
				
				return ReturnMessage.SUCCESS;
		} catch (CorruptIndexException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostName + "...");
			Util.deleteDir(new File(pathDir+"/"+hostName));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} catch (IOException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostName + "...");
			Util.deleteDir(new File(pathDir+"/"+hostName));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} 
	}
	
	private IndexWriter getIndexWriter(String hostName) throws CorruptIndexException, LockObtainFailedException, IOException{
		return new IndexWriter(getDiretory(hostName), ANALYZER,	MaxFieldLength.UNLIMITED);
	}

	private Directory getDiretory(String hostName) throws IOException {
		Directory dir = FSDirectory.open(new File(pathDir + "/" + hostName));
		return dir;
	}

}
