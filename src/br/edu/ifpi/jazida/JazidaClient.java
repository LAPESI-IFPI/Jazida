package br.edu.ifpi.jazida;

import static br.edu.ifpi.jazida.util.FileUtilsForTest.conteudoDoArquivo;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import br.edu.ifpi.jazida.client.TextIndexerClient;
import br.edu.ifpi.jazida.client.TextSearcherClient;
import br.edu.ifpi.opala.searching.ResultItem;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.MetaDocumentBuilder;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class JazidaClient {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			printUsage();
		} else if(args[0].equals("index")) {
			ReturnMessage message = indexFile(new File(args[1]));
			System.out.println(message.getMessage());
		} else if (args[0].equals("search")){
			ReturnMessage message = searchText(args[1]);
			System.out.println(message.getMessage());
		} else {
			printUsage();
		}
	}

	private static void printUsage() {
		System.out.println("Paramêtros possíveis:");
		System.out.println("index <filename>");
		System.out.println("search <query>");
	}

	private static ReturnMessage searchText(String query) throws Exception {
		System.out.println("Carregando cliente de busca de texto...");
		TextSearcherClient textSeacherClient = new TextSearcherClient();
		
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.CONTENT.getValue(), query);
		fields.put(Metadata.TITLE.getValue(), query);
		fields.put(Metadata.ID.getValue(), query);
		
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.AUTHOR.getValue());
		returnedFields.add(Metadata.TITLE.getValue());
		
		SearchResult resultado = textSeacherClient.search(fields, returnedFields, 1, 10, Metadata.ID.getValue(), false);
		textSeacherClient.close();
		
		if(resultado.getItems().size() == 0 ) {
			System.out.println("Nenhum resultado para sua consulta.");
		} else {
			System.out.println("Restultados para " + query + ":");
			for(ResultItem item: resultado.getItems()) {
				System.out.println(item.getId());
			}
		}
		
		return resultado.getCodigo();
	}

	private static ReturnMessage indexFile(File file) throws Exception {
		System.out.println("Carregando cliente de indexação de texto...");
		TextIndexerClient textIndexerClient = new TextIndexerClient();
		
		
		String text = conteudoDoArquivo(file);
		
		MetaDocument metaDocument = new MetaDocumentBuilder()
												.id(file.getName())
												.title(file.getName())
												.build();
		
		ReturnMessage message = textIndexerClient.addText(metaDocument, text);
		return message;
	}
}
