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
	private static ImageIndexReply imageIndexReply = new ImageIndexReply();
	
	private ImageIndexReply() {
	}
	
	public static ImageIndexReply getTextIndexUtil() {
		if(imageIndexReply == null) {
			imageIndexReply = new ImageIndexReply();
		}
		return imageIndexReply;
	}


	public synchronized ReturnMessage addImageReply(MetaDocument metaDocument, BufferedImage image, String hostname,
			String IP, long numDocsIndex) throws IOException {
		
		DocumentBuilder builder = DocumentBuilderFactory.getCEDDDocumentBuilder();
		Document doc = builder.createDocument(image, metaDocument.getId());
		List<Fieldable> fields = doc.getFields();
		for (Fieldable field : fields) {
			metaDocument.getDocument().add(field);
		}

		try {			
			IndexWriter writer = getIndexWriter(hostname);			
			writer.addDocument(metaDocument.getDocument());
			System.out.println("add reply imagae");
			long numDocsReply = writer.numDocs();
			writer.close();
			
			if (numDocsIndex > numDocsReply){
				LOG.info("Atualizando réplica do "+ hostname + "...");
				new SupportReplyImage().startUpdateIndexReply(IP, getDiretory(hostname), HOSTNAME_LOCAL);
				System.out.println("replica atualizada imagem: metodo 1");
				return ReturnMessage.OUTDATED;
			}
		} catch (CorruptIndexException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostname + "...");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} catch (IOException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostname + "...");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} 

		return ReturnMessage.SUCCESS;
	}
	
	public synchronized ReturnMessage delImageReply(String id, String hostname, String IP) throws IOException {
		IndexReader reader;
		Directory dir = null;
		try {
			dir = getDiretory(hostname);
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
			System.out.println("del reply imagage");
			reader.close();
			dir.close();
			
			return ReturnMessage.SUCCESS;
		} catch (CorruptIndexException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostname + "...");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} catch (IOException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostname + "...");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} 
		
	}
	
	public synchronized ReturnMessage updateImageReply(String id,
			Map<String, String> updates, String hostname, String IP) throws IOException {
		try {
			Directory dir = getDiretory(hostname);
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
				
				if (!this.delImageReply(id, hostname, IP).equals(ReturnMessage.SUCCESS)) {
					return ReturnMessage.ID_NOT_FOUND;
				}

				IndexWriter writer = getIndexWriter(hostname);
				writer.addDocument(doc);
				writer.optimize();
				writer.close();
				System.out.println("atualizou reply image");
				return ReturnMessage.SUCCESS;
		} catch (CorruptIndexException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostname + "...");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
			return ReturnMessage.UNEXPECTED_INDEX_ERROR;
		} catch (IOException e) {
			LOG.info("Restaurando réplica de imagem do "+ hostname + "...");
			Util.deleteDir(new File(pathDir+"/"+hostname));
			new SupportReplyImage().startRestoreIndexReply(IP, HOSTNAME_LOCAL);
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

}
