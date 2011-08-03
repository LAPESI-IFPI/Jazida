package br.edu.ifpi.jazida;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import br.edu.ifpi.jazida.client.TextSearcherClient;
import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.opala.searching.ResultItem;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.QueryMapBuilder;
/**
 * Realiza interações com um cluster Jazida através da linha de comando.
 * 
 * @author aecio
 *
 */
public class Jazida {
	
	
	public static void main(String[] args) throws Exception {
		if(args[0] == null) {
			System.out.println("Informe um dos metodos a ser invocado: startNode, search");
		}
		if(args[0].equals("startNode")) {
			startDataNode(args);
		}
		else if(args[0].equals("search")) {
			if(args.length < 2) {
				System.out.println("Uso: search <query>");
				System.exit(1);
			}
			search(args[1]);
		}
	}

	private static void startDataNode(String[] args) {
		final DataNode datanode = new DataNode();
		try {
			if(args.length == 1) {
				datanode.start();
			}else {
				try {
					datanode.start(	args[1],
								InetAddress.getLocalHost().getHostAddress(),
								Integer.parseInt(args[2]),
								Integer.parseInt(args[3]),
								Integer.parseInt(args[4]),
								Integer.parseInt(args[5]),
								Integer.parseInt(args[6]),
								Integer.parseInt(args[7]),
								Integer.parseInt(args[8]),
								Integer.parseInt(args[9]),
								true);
				}catch (NumberFormatException e) {
					System.out.println("Uso: startNode <NodeName> <TextIndexerPort> <TextSearcherPort> <ImageIndexerPort> <ImageSearcherPort>");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void search(String string) throws Exception {
		TextSearcherClient searcher = new TextSearcherClient();
		Map<String, String> fields = new QueryMapBuilder()
												.id(string)
												.author(string)
												.title(string)
												.content(string)
												.build();
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		
		long inicio = System.currentTimeMillis();
		SearchResult result = searcher.search(fields, returnedFields, 1, 10, null);
		long fim = System.currentTimeMillis();
		
		if(result.getItems().size() > 0) {
			System.out.println("Documentos encontrados:");
			int i=0;
			for (ResultItem hit : result.getItems()) {
				i++;
				System.out.println(hit.getScore()+" - ID: "+hit.getId() + " - TITULO: "+hit.getField(Metadata.TITLE.getValue()));
			}
		}else {
			System.out.println("Nenhum documento encontrado.");
		}
		System.out.println("Busca executada em "+(fim-inicio)+" ms.");
		searcher.close();
		System.exit(1);
	}
}
