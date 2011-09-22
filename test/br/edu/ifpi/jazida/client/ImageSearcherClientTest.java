package br.edu.ifpi.jazida.client;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.jazida.util.PathJazida;
import br.edu.ifpi.opala.searching.ResultItem;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.searching.SearcherImage;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.MetaDocumentBuilder;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.QueryMapBuilder;
import br.edu.ifpi.opala.utils.ReturnMessage;
import br.edu.ifpi.opala.utils.Util;

public class ImageSearcherClientTest {
	
	public static final File IMAGE_01 = new File("./sample-data/images/image01.bmp");
	public static final File IMAGE_02 = new File("./sample-data/images/image02.jpg");
	public static final File IMAGE_03 = new File("./sample-data/images/image03.bmp");
	public static final File IMAGE_03_DUPLICADA = new File("./sample-data/images/image03.duplicada.bmp");
	private static DataNode datanode;

	@BeforeClass
	public static void setUp() throws Exception {
		assertTrue(Util.deleteDir(new File(Path.IMAGE_INDEX.getValue())));
		assertTrue(Util.deleteDir(new File(Path.IMAGE_BACKUP.getValue())));
		assertTrue(Util.deleteDir(new File(PathJazida.IMAGE_INDEX_REPLY.getValue())));
		datanode = new DataNode();
		datanode.start(false);
	
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		datanode.stop();
	}

	@Test
	public void deveriaMarcarImagensDeMesmoScoreComoDuplicada() throws IOException, KeeperException, InterruptedException {
		//dado
		dadoQueImagem03FoiIndexada();
		dadoQueImagem03DuplicadaFoiIndexada();
		SearcherImage searcher = new ImageSearcherClient();
		//quando
		SearchResult searchResult = searcher.search(ImageIO.read(IMAGE_03), 10);

		//então
		assertThat(searchResult.getCodigo(), is(ReturnMessage.SUCCESS));
		assertThat(searchResult.getItems().size(), is(2));
		assertThatDuplicatedDocumentsHaveSameScore(searchResult);
	}
	
	private static void assertThatDuplicatedDocumentsHaveSameScore(SearchResult searchResult) {
		List<ResultItem> docs = searchResult.getItems();
		for (int i=1; i < docs.size(); i++) {
			if (docs.get(i).isDuplicated()) {
				assertTrue(docs.get(i-1).getScore().equals(docs.get(i).getScore()));
			}
		}
	}

	@Test
	public void deveriaEncontrarImagemIndexaEDevolverSuccesss() throws IOException, KeeperException, InterruptedException {
		//dado
		dadoQueImagem01FoiIndexada();
		dadoQueImagem02FoiIndexada();
		SearcherImage searcher = new ImageSearcherClient();	
		
		//quando
		SearchResult searchResult= searcher.search(ImageIO.read(IMAGE_01), 1);
		
		//então
		assertThat(searchResult.getCodigo(), is(ReturnMessage.SUCCESS));		
		assertThat(searchResult.getItems().iterator().next().getId(), is("01"));		
	}
	
	@Test
	public void deveriaDevolverParameterInvalidQuandoImagemEhNula() throws IOException, KeeperException, InterruptedException {
		//dado
		SearcherImage searcher = new ImageSearcherClient();
		
		//quando
		SearchResult searchResult= searcher.search(null, 10);
		
		//então
		assertThat(searchResult.getCodigo(), is(ReturnMessage.PARAMETER_INVALID));		
	}

	@Test
	public void deveriaRetornarSomenteAQuantidadeDeImagemPedida() throws IOException, KeeperException, InterruptedException {
		//dado	
		SearcherImage searcher = new ImageSearcherClient();
		
		//quando
		SearchResult searchResult= searcher.search(ImageIO.read(IMAGE_03), 2);
		
		//então
		assertThat(searchResult.getCodigo(), is(ReturnMessage.SUCCESS));		
		assertThat(searchResult.getItems().size(), is(2));		
	}
	

	/**
	 * Testa a busca com fields vazio e espera INVALID_QUERY
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * @throws IOException 
	 */
	@Test
	public void deveriaDevolverInvalidQueryQuandoFieldsEhUmMapaVazio() throws IOException, KeeperException, InterruptedException {
		//dado
		SearcherImage searcher = new ImageSearcherClient();
		Map<String, String> fields = new HashMap<String, String>();
		List<String> returnedFields = new ArrayList<String>();
		
		//quando
		SearchResult searchResult = searcher.search(fields, returnedFields, 1, 10, null, false);
		
		//então
		assertThat(searchResult.getCodigo(), is(ReturnMessage.INVALID_QUERY));
	}

	/**
	 * Testa o envio de returnedFields vazio e espera SUCCESS
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * @throws IOException 
	 */
	@Test
	public void deveriaDevolverSuccessQuandoReturnedFieldsEhVazio() throws IOException, KeeperException, InterruptedException {
		//dado
		SearcherImage searcher = new ImageSearcherClient();
		Map<String, String> fields = new HashMap<String, String>();
		fields.put(Metadata.ID.getValue(), "01");
		List<String> returnedFields = new ArrayList<String>();
		
		//quando
		SearchResult searchResult = searcher.search(fields, returnedFields, 1, 10, null, false);
		
		//então
		assertEquals(ReturnMessage.SUCCESS, searchResult.getCodigo());
	}

	/**
	 * Testa a busca com um returnedField não-existente e espera NULL
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * @throws IOException 
	 */
	@Test
	public void deveriaBuscarcomOReturnedFieldInexistenteERetornarNull() throws IOException, KeeperException, InterruptedException {
		//dado
		SearcherImage searcher = new ImageSearcherClient();
		Map<String, String> fields = new QueryMapBuilder().keywords("image por do sol").build();
		List<String> returnedFields = new ArrayList<String>();
		
		returnedFields.add("diaemqueoautornasceu");
		//quando
		SearchResult searchResult = searcher.search(fields, returnedFields, 1, 10, null, false);
		
		//então
		assertThat(searchResult.getCodigo(), is(ReturnMessage.SUCCESS));
		assertNull(searchResult.getItem(0).getField("diaemqueoautornasceu"));
	}

	/**
	 * Teste que verifica se um documento marcado como duplicado tem o mesmo
	 * score que o seu anterior.
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 * @throws IOException 
	 */
	@Test
	public void searchIdenticalDocuments() throws IOException, KeeperException, InterruptedException {
		//dado		
		SearcherImage searcher = new ImageSearcherClient();
		Map<String, String> fields = new QueryMapBuilder().keywords("passaros").build();
		List<String> returnedFields = new ArrayList<String>();
		
		//quando
		SearchResult searchResult = searcher.search(fields, returnedFields, 1, 10, null, false);
		
		//então
		assertEquals(ReturnMessage.SUCCESS, searchResult.getCodigo());
		assertThatDuplicatedDocumentsHaveSameScore(searchResult);
	}
//
//	/**
//	 * Testa busca por termo não existente no índice. Espera-se que não
//	 * retorne nenhum documento.
//	 */
//	@Test
//	public void deveriaDevolverEmptySearcherQuandoQueryNaoExisteNoIndice() {
//		fields.put(Metadata.TITLE.getValue(), "Xinforimpuladodannylvan");
//		searchResult = searcher.search(fields, returnedFields, 1, 10, null,
//				false);
//		assertEquals(ReturnMessage.EMPTY_SEARCHER, searchResult.getCodigo());
//		assertEquals(0, searchResult.getItems().size());
//	}
//
//	
//	/**
//	 * Testa busca com query inválida e espera INVALID_QUERY
//	 */
//	@Test
//	public void searchInvalidQuery() {
//		fields.put("[author", "Tavares da Silva");
//		searchResult = searcher.search(fields, returnedFields, 1, 10, null, false);
//		assertEquals(ReturnMessage.INVALID_QUERY, searchResult.getCodigo());
//	}

	public static  void dadoQueImagem01FoiIndexada() throws IOException, KeeperException, InterruptedException {
		MetaDocument metadoc = new MetaDocumentBuilder()
											.id("01")
											.keywords("image por do Sol")
											.build();
		BufferedImage image = ImageIO.read(IMAGE_01);
		new ImageIndexerClient().addImage(metadoc, image);
	}

	public static  void dadoQueImagem02FoiIndexada() throws IOException, KeeperException, InterruptedException {
		MetaDocument metadoc = new MetaDocumentBuilder()
											.id("02")
											.keywords("Floresta azul")
											.build();
		BufferedImage image = ImageIO.read(IMAGE_02);
		
		new ImageIndexerClient().addImage(metadoc, image);
	}
	
	public static  void dadoQueImagem03FoiIndexada() throws IOException, KeeperException, InterruptedException {
		MetaDocument metadoc = new MetaDocumentBuilder()
										.id("03")
										.keywords("Passaros voando.")
										.build();
		BufferedImage image = ImageIO.read(IMAGE_03);
		new ImageIndexerClient().addImage(metadoc, image);
	}
	
	public static  void dadoQueImagem03DuplicadaFoiIndexada() throws IOException, KeeperException, InterruptedException {
		MetaDocument metadoc = new MetaDocumentBuilder()
										.id("03_DUPLICADA")
										.keywords("Passaros voando.")
										.build();
		BufferedImage image = ImageIO.read(IMAGE_03_DUPLICADA);
		new ImageIndexerClient().addImage(metadoc, image);
	}
}
