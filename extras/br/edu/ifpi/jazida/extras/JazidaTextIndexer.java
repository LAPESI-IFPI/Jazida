package br.edu.ifpi.jazida.extras;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import br.edu.ifpi.jazida.client.TextIndexerClient;
import br.edu.ifpi.opala.utils.MetaDocument;

public class JazidaTextIndexer {

	private static File pasta = new File("/home/aecio/workspace/lapesi/dez-mil-txt");
	private static File[] arquivos = pasta.listFiles();
	private static int atual = -1;

	public static void main(String[] args) throws IOException, SecurityException, NoSuchMethodException {
		
		long inicio = System.currentTimeMillis();
		try {
			TextIndexerClient jazidaClient = new TextIndexerClient();
		
			File arquivo = nextDocument();
			while( arquivo != null ) {
				MetaDocument metadoc = new MetaDocument();
				metadoc.setTitle(arquivo.getName());
				metadoc.setId(arquivo.getName());
				
				//Ler conteúdo do arquivo
				FileInputStream stream;
				stream = new FileInputStream(arquivo);
				InputStreamReader streamReader = new InputStreamReader(stream);
				BufferedReader reader = new BufferedReader(streamReader);
				
				StringBuffer stringBuffer = new StringBuffer();
				String line;
				while ((line = reader.readLine()) != null) {
					stringBuffer.append(line);
				}
				arquivo = nextDocument();
				
				jazidaClient.addText(metadoc, stringBuffer.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
			
			
		
		long fim = System.currentTimeMillis();
		long total = fim - inicio;
		
		System.out.println(total);
	
	}

	private static File nextDocument() {
		atual++;
		if (atual < arquivos.length) {
			return arquivos[atual];
		} else {
			return null;
		}
	}
}
