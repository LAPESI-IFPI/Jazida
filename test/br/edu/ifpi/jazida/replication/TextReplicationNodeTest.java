package br.edu.ifpi.jazida.replication;

import static br.edu.ifpi.jazida.util.FileUtilsForTest.conteudoDoArquivo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.queryParser.ParseException;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import br.edu.ifpi.jazida.client.TextIndexerClient;
import br.edu.ifpi.jazida.client.TextSearcherClient;
import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.jazida.util.FileUtilsForTest;
import br.edu.ifpi.jazida.util.PathJazida;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.searching.TextSearcher;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.MetaDocumentBuilder;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.QueryMapBuilder;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class TextReplicationNodeTest {

	private static DataNode datanode;

	@BeforeClass
	public static void setUpTest() throws Exception {

		assertTrue(FileUtilsForTest.deleteDir(new File(Path.TEXT_INDEX
				.getValue())));
		assertTrue(FileUtilsForTest.deleteDir(new File(Path.TEXT_BACKUP
				.getValue())));
		assertTrue(FileUtilsForTest.deleteDir(new File(PathJazida.TEXT_INDEX_REPLY.getValue())));
		
		datanode = new DataNode();
		datanode.start(false);
	}

	@AfterClass
	public static void tearDownTest() throws InterruptedException {
		datanode.stop();
	}

	@Test
	public void deveriaReplicarViaIndexcaoOsIndeciesDeTextoEImagemNoIndiceDistribuido()
			throws IOException, InterruptedException, KeeperException {
		indexaArquivos();
	}
	
	@Test
	public void deveriaDevolverSuccessEAtualizarOsValoresDoDocumento() throws IOException, ParseException, KeeperException, InterruptedException {
		//dado
		String tituloAtualizado ="Alice's Adventures in Wonderland Revisado e Atualizado 2ª Ed"; 
		Map<String, String> novosMetadados = new QueryMapBuilder()
													.title(tituloAtualizado)
													.build();
		
		Map<String, String> query = new QueryMapBuilder()
											.title("Adventures")
											.build();
		
		Map<String, String> queryAtualizado = new QueryMapBuilder()
												.title("Atualizado")
												.build();
												
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());
		
		TextIndexerClient textIndexerClient = new TextIndexerClient();
		TextSearcher searcher = new TextSearcherClient();
		
		//quando
		SearchResult beforeUpdate = searcher.search(query, returnedFields , 0, 10, null);
		
		ReturnMessage resultUpdate = textIndexerClient.updateText("1", novosMetadados);
		
		SearchResult afterUpdate = searcher.search(queryAtualizado, returnedFields, 0, 10, null);
		
		
		//entao
		assertThat(beforeUpdate.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(theFirstTitleOfSearchResult(beforeUpdate), is("Alice's Adventures in Wonderland"));
		assertThat(resultUpdate, is(ReturnMessage.SUCCESS));
		assertThat(afterUpdate.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(theFirstTitleOfSearchResult(afterUpdate), is(tituloAtualizado));
		
		
		}

	
	@Test
	public void deveriaDeletarNoIndiceENoIndiceDasReplicasDeTexto()
			throws IOException, InterruptedException, KeeperException {
		TextIndexerClient textIndexerClient = new TextIndexerClient();
		
		ReturnMessage deletionReturnMessage = textIndexerClient.delText("2");
		ReturnMessage deletioneturnMessage = textIndexerClient.delText("2");
		assertThat(deletionReturnMessage, is(ReturnMessage.SUCCESS));
		assertThat(deletioneturnMessage, is(ReturnMessage.ID_NOT_FOUND));
	}
	
	
	
	private String theFirstTitleOfSearchResult(SearchResult beforeUpdate) {
		return beforeUpdate.getItem(0).getField(Metadata.TITLE.getValue());
	}

	private void indexaArquivos() throws KeeperException, InterruptedException,
			IOException {

		File ALICE_TXT_FILE = new File("./sample-data/texts/alice.txt");
		File OPALA_FILE = new File("./sample-data/texts/opala.txt");
		File AMERICA_M_TXT_FILE = new File("./sample-data/texts/Americana_M.txt");
		File AMERICANA_M2_TXT_FILE = new File("./sample-data/texts/Americana_M2.txt");
		
		TextIndexerClient textIndexerClient = new TextIndexerClient();

		MetaDocument metaDoc = new MetaDocumentBuilder().id("1")
				.title("Alice's Adventures in Wonderland").author("autor").build();

		MetaDocument metaDoc2 = new MetaDocumentBuilder().id("2")
				.title("OPALA_FILE").author("autor02").build();

		MetaDocument metaDoc3 = new MetaDocumentBuilder().id("3")
				.title("AMERICA_M_TXT_FILE").author("autor03").build();

		MetaDocument metaDoc4 = new MetaDocumentBuilder().id("4")
				.title("AMERICANA_M2_TXT_FILE").author("autor04").build();

	
		textIndexerClient.addText(metaDoc, conteudoDoArquivo(ALICE_TXT_FILE));
		textIndexerClient.addText(metaDoc2, conteudoDoArquivo(OPALA_FILE));
		textIndexerClient.addText(metaDoc3,	conteudoDoArquivo(AMERICA_M_TXT_FILE));
		textIndexerClient.addText(metaDoc4,	conteudoDoArquivo(AMERICANA_M2_TXT_FILE));

	}
}
