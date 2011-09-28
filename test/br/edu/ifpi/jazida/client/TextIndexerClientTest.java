package br.edu.ifpi.jazida.client;

import static br.edu.ifpi.jazida.util.FileUtilsForTest.conteudoDoArquivo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.queryParser.ParseException;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

/**
 * Teste para {@link TextIndexerClient}. Para funcionar, os Serviço do Zookeeper
 * deve estar iniciado.
 * 
 * @author Aécio Solano Rodrigues Santos
 */
public class TextIndexerClientTest {

	private static final File ALICE_TXT_FILE = new File("./sample-data/texts/alice.txt");
	private static String TITLE = "Alice's Adventures in Wonderland";
	private static String AUTHOR = "Lewis Carroll";
	private static String DOCUMENT_ID = "alice";
	
	private static DataNode datanode;

	@BeforeClass
	public static void setUpTest() throws Exception {
		assertTrue(FileUtilsForTest.deleteDir(new File(Path.TEXT_INDEX.getValue())));
		assertTrue(FileUtilsForTest.deleteDir(new File(Path.TEXT_BACKUP.getValue())));
		assertTrue(FileUtilsForTest.deleteDir(new File(PathJazida.TEXT_INDEX_REPLY.getValue())));
		datanode = new DataNode();
		datanode.start(false);
	}
	
	@AfterClass
	public static void tearDownTest() throws InterruptedException {
		datanode.stop();
	}

	@Before
	public void setUp() {
		//		
		//Apagar o índice para cada teste
		//
//		assertTrue(FileUtilsForTest.deleteDir(new File(Path.TEXT_INDEX.getValue())));
	}

	@Test
	public void deveriaAdicionarUmDocumentoDeTextoNoIndiceDistribuido()
	throws KeeperException, InterruptedException, IOException {
		// Dado
		TextIndexerClient textIndexerClient = new TextIndexerClient();
		
		MetaDocument metaDocumento = new MetaDocumentBuilder()
												.id(ALICE_TXT_FILE.getName())
												.title(TITLE)
												.author(AUTHOR)
												.build();
		
		String texto = conteudoDoArquivo(ALICE_TXT_FILE);
		dadoQueOArquivoAliceTxtFoiIndexadoComIdAlice();
		dadoQueOArquivoAliceTxtFoiIndexadoComIdAlice();
		dadoQueOArquivoAliceTxtFoiIndexadoComIdAlice();
		dadoQueOArquivoAliceTxtFoiIndexadoComIdAlice();
		
		// Quando
		ReturnMessage message = textIndexerClient.addText(metaDocumento, texto);
		// Então
		assertThat(message, is(ReturnMessage.SUCCESS));
	}
	
	@Test
	public void naoDeveriaEncontrarDocumentoDepoisDeSerRemovidoDoIndice()
	throws FileNotFoundException, IOException, KeeperException, InterruptedException {
		//dado
		//dadoQueOArquivoAliceTxtFoiIndexadoComIdAlice();
		TextIndexerClient textIndexerClient = new TextIndexerClient();
		
		//quando
		ReturnMessage deletionReturnMessage = textIndexerClient.delText(DOCUMENT_ID);
		ReturnMessage deletionNotFound = textIndexerClient.delText(DOCUMENT_ID);

		//entao
		assertThat(deletionReturnMessage, is(ReturnMessage.SUCCESS));
		assertThat(deletionNotFound, is(ReturnMessage.ID_NOT_FOUND));
	}

	@Test
	public void deveriaDevolverIdNotFoundQuandoDeletadoDocumentoNaoIndexado() throws FileNotFoundException, IOException, KeeperException, InterruptedException {
		//dado
		TextIndexerClient textIndexerClient = new TextIndexerClient();
		
		//quando
		ReturnMessage returnedMessage = textIndexerClient.delText("ID inexistente no indice");
		
		//entao
		assertThat(returnedMessage, is(ReturnMessage.ID_NOT_FOUND));
	}
	
	@Test
	public void deveriaDevolverSuccessEAtualizarOsValoresDoDocumento() throws IOException, ParseException, KeeperException, InterruptedException {
		//dado
		dadoQueOArquivoAliceTxtFoiIndexadoComIdAlice();
		
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
		
		ReturnMessage resultUpdate = textIndexerClient.updateText(DOCUMENT_ID, novosMetadados);
		
		SearchResult afterUpdate = searcher.search(queryAtualizado, returnedFields, 0, 10, null);
		
		//entao
		assertThat(beforeUpdate.getCodigo(), is(ReturnMessage.SUCCESS));
		
		assertThat(theFirstTitleOfSearchResult(beforeUpdate),
				is("Alice's Adventures in Wonderland"));
		
		assertThat(resultUpdate, is(ReturnMessage.SUCCESS));
		
		assertThat(afterUpdate.getCodigo(), is(ReturnMessage.SUCCESS));

		assertThat(theFirstTitleOfSearchResult(afterUpdate),
				is(tituloAtualizado));
	}

	private String theFirstTitleOfSearchResult(SearchResult beforeUpdate) {
		return beforeUpdate.getItem(0).getField(Metadata.TITLE.getValue());
	}
	
	private void dadoQueOArquivoAliceTxtFoiIndexadoComIdAlice()
	throws KeeperException, InterruptedException, IOException {
		
		MetaDocument metaDoc = new MetaDocumentBuilder()
									.id(DOCUMENT_ID)
									.title(TITLE)
									.author(AUTHOR)
									.build();
		
		TextIndexerClient textIndexerClient = new TextIndexerClient();
		textIndexerClient.addText(metaDoc, conteudoDoArquivo(ALICE_TXT_FILE));
		
	}


}
