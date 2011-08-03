package br.edu.ifpi.jazida.extras;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.MetaDocumentBuilder;

class WikipediaFileReader {

	private BufferedReader reader;
	private int lineNumber = 0;
	private int hash = this.hashCode();
	private Object lineNumberSignal = new Object();
	
	public WikipediaFileReader(File file) throws IOException {
		this.reader = new BufferedReader(new FileReader(file));
	}
	
	public void close() throws IOException {
		reader.close();
	}
	
	public WikiDocument nextDocument() throws IOException {
		String line;
		WikiDocument doc = null;
		while(doc == null) {
			synchronized(reader) {
				line = reader.readLine();
				if(line == null) {
					return null;
				}
			}
			String id;
			synchronized (lineNumberSignal) {
				lineNumber++;
				id = hash +"-"+lineNumber;
			}
			doc = parseWikiDocument(id, line);
		}
		return doc;
	}

	private WikiDocument parseWikiDocument(String id, String linha) {
		try {
			StringTokenizer tokenizer = new StringTokenizer(linha, "\t");
			
			String title = tokenizer.nextToken();
			String date = tokenizer.nextToken();
			String content = tokenizer.nextToken();
			
			MetaDocument metadoc = new MetaDocumentBuilder()
										.id(id)
										.title(title)
										.publicationDate(date)
										.build();
			
			return new WikiDocument(metadoc, content);
			
		}catch (NoSuchElementException e) {
			System.out.println("Falhou ao ler documento "+id);
		}
		return null;
	}
	
	public class WikiDocument {
		private final MetaDocument metadoc;
		private final String content;

		public WikiDocument(MetaDocument metadoc, String conteudo) {
			this.metadoc = metadoc;
			this.content = conteudo;
		}
		public MetaDocument getMetadoc() {
			return metadoc;
		}
		public String getContent() {
			return content;
		}
	}

}
