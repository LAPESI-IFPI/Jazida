package br.edu.ifpi.jazida.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

import br.edu.ifpi.opala.indexing.NearRealTimeTextIndexer;
import br.edu.ifpi.opala.indexing.TextIndexer;
import br.edu.ifpi.opala.indexing.TextIndexerImpl;
import br.edu.ifpi.opala.indexing.parser.TxtParser;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class UtilForTest {
	
	public static final Logger LOG = Logger.getLogger(UtilForTest.class);
	
	public static MetaDocument novoMetaDocumento(String title, String author, File arquivo) {
		MetaDocument metadoc = new MetaDocument();
		metadoc.setTitle(title);
		metadoc.setAuthor(author);
		metadoc.setId(arquivo.getName());
		return metadoc;
	}

	/**
	 * Indexa um arquivo txt ou todos os arquivos txt do diretório e dos
	 * subdiretórios, caso contrário imprime o nome dos arquivos que não puderam
	 * ser indexados. Os metadados indexados são os padrões
	 * 
	 * @param file
	 *            - arquivo ou diretório a ser indexado
	 * @return - true se conseguir indexar ao menos um arquivo
	 */
	public static boolean indexTextDirOrFile(File dir) {
		boolean result = false;
		if (dir.isDirectory()) {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (indexTextDirOrFile(file))
					result = true;
			}
		} else {
			String name = dir.getName();
			int position = name.lastIndexOf(".");
			if (position == -1
					|| !name.substring(position).toUpperCase().equals(".TXT")) {
				return false;
			}
			InputStream is;
			try {
				is = new FileInputStream(dir);
				String texto = new TxtParser().getContent(is);
				is.close();
				if (texto == null) {
					LOG.info("O documento \"" + name + "\" não pode ser extraído.");
				} else {
					TextIndexer textIndexer = TextIndexerImpl.getTextIndexerImpl();
					MetaDocument metaDoc = createMetaDocument(name);
					if (textIndexer.addText(metaDoc, texto).equals(ReturnMessage.SUCCESS)
						|| textIndexer.addText(metaDoc, texto).equals(ReturnMessage.DUPLICATED_ID)) {
						return true;
					}
				}
			} catch (FileNotFoundException e) {
				LOG.error(e);
				return false;
			} catch (IOException e) {
				LOG.error(e);
				return false;
			}
			return false;
		}
		return result;
	}
	
	/**
	    * Indexa um arquivo txt ou todos os arquivos txt do diretório e
	    * dos subdiretórios, utilizando a classe NearRealTimeTextIndexer,
	    * caso contrário imprime o nome dos arquivos que não puderam
	    * ser indexados. Os metadados indexados são os padrões
	    * @param file - arquivo ou diretório a ser indexado 
	    * @return - true se conseguir indexar ao menos um arquivo
	    */
	public static boolean indexTextDirOrFile(File dir, NearRealTimeTextIndexer indexer) {
		boolean result = false;
		if (dir.isDirectory()) {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (indexTextDirOrFile(file, indexer))
					result = true;
			}
		}
		else {
			String name = dir.getName();
			int position = name.lastIndexOf(".");
			if (position==-1 || !name.substring(position).toUpperCase().equals(".TXT")){
				return false;
			}
			InputStream is;
			try {
				is = new FileInputStream(dir);
				String texto = new TxtParser().getContent(is);
				is.close();
				if(texto==null){
					System.out.println("O documento \""+name+"\" não pode ser EXTRAÍDO");
				} else {
					
					MetaDocument metaDoc = createMetaDocument(name);
					if (indexer.addText(metaDoc, texto).equals(ReturnMessage.SUCCESS) || indexer.addText(metaDoc, texto).equals(ReturnMessage.DUPLICATED_ID))
						return true;
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				return false;
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
			return false;
		}
		return result;
	}
	
	/**
	 * Método que cria um MetaDocument com os metadados padrões de indexação
	 * @return - MetaDocument
	 */		
	public static MetaDocument createMetaDocument(String id){
		MetaDocument md = new MetaDocument();
		md.setId(id);
		md.setAuthor("AUTHOR "+id);
		md.setFormat("FORMAT "+id);
		md.setTitle("TITLE "+id);
		md.setKeywords("Keywords "+id);
		md.setPublicationDate("10/02/2010");
		md.setType("Type "+id);
		md.setField("sortable", id, false);
		
		return md;
	}
}
