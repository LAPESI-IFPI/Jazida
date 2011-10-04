package br.edu.ifpi.jazida.replication;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.lucene.queryParser.ParseException;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import br.edu.ifpi.jazida.client.ImageIndexerClient;
import br.edu.ifpi.jazida.client.ImageSearcherClient;
import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.jazida.util.FileUtilsForTest;
import br.edu.ifpi.jazida.util.PathJazida;
import br.edu.ifpi.opala.indexing.ImageIndexer;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.MetaDocumentBuilder;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.QueryMapBuilder;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class ImageReplicationNodeTest {
	
	public static final File IMAGE_01 = new File(
			"./sample-data/images/image01.bmp");
	public static final File IMAGE_02 = new File(
			"./sample-data/images/image02.jpg");
	public static final File IMAGE_03 = new File(
			"./sample-data/images/image03.bmp");
	public static final File IMAGE_03_DUPLICADA = new File(
			"./sample-data/images/image03.duplicada.bmp");
	private static DataNode datanode;

	@BeforeClass
	public static void setUpTest() throws Exception {

		assertTrue(FileUtilsForTest.deleteDir(new File(Path.IMAGE_INDEX
				.getValue())));
		assertTrue(FileUtilsForTest.deleteDir(new File(Path.IMAGE_BACKUP
				.getValue())));
		assertTrue(FileUtilsForTest.deleteDir(new File(
				PathJazida.IMAGE_INDEX_REPLY.getValue())));
		
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
		 indexaImagens();
	}

	@Test
	public void deveriaDevolverSuccessEAtualizarOsValoresDoDocumento()
			throws IOException, ParseException, KeeperException,
			InterruptedException {
		String tituloAtualizado = "Novo titulo IMAGEM01";
		Map<String, String> novosMetadados = new QueryMapBuilder().title(tituloAtualizado).build();
		List<String> returnedFields = new ArrayList<String>();
		returnedFields.add(Metadata.TITLE.getValue());

		ImageIndexerClient imageIndexerClient = new ImageIndexerClient();
		ImageSearcherClient searcher = new ImageSearcherClient();
		
		ReturnMessage resultUpdate = imageIndexerClient.updateImage("01", novosMetadados);
		
		SearchResult afterUpdate = searcher.search(novosMetadados, returnedFields, 1, 10, null, false);
		
		
		assertThat(resultUpdate, is(ReturnMessage.SUCCESS));
		assertThat(theFirstTitleOfSearchResult(afterUpdate), is(tituloAtualizado));
		
	}
	
	private String theFirstTitleOfSearchResult(SearchResult beforeUpdate) {
		return beforeUpdate.getItem(0).getField(Metadata.TITLE.getValue());
	}
	
	@Test
	public void deveriaDeletarNoIndiceENoIndiceDasReplicasDeTexto()
			throws IOException, InterruptedException, KeeperException {
		ImageIndexerClient imageIndexerClient = new ImageIndexerClient();

		ReturnMessage deletionReturnMessage = imageIndexerClient.delImage("02");
		ReturnMessage deletionReturnMessage2 = imageIndexerClient.delImage("03");
		assertThat(deletionReturnMessage, is(ReturnMessage.SUCCESS));
		assertThat(deletionReturnMessage2, is(ReturnMessage.SUCCESS));
	}

	private void indexaImagens() throws IOException, KeeperException,
			InterruptedException {
		MetaDocument metadoc = new MetaDocumentBuilder().id("01")
		.title("IMAGEM01").keywords("image por do sol").build();
		BufferedImage image = ImageIO.read(IMAGE_01);

		MetaDocument metadoc2 = new MetaDocumentBuilder().id("02")
				.title("IMAGEM02").keywords("image por do sol").build();
		BufferedImage image2 = ImageIO.read(IMAGE_02);

		MetaDocument metadoc3 = new MetaDocumentBuilder().id("03")
				.title("IMAGEM03").keywords("image por do sol").build();
		BufferedImage image3 = ImageIO.read(IMAGE_03);

		MetaDocument metadoc4 = new MetaDocumentBuilder().id("03_DUPLICADA")
				.title("IMAGEM04").keywords("image por do sol").build();
		BufferedImage image4 = ImageIO.read(IMAGE_03_DUPLICADA);

		ImageIndexer indexer = new ImageIndexerClient();
		indexer.addImage(metadoc, image);
		indexer.addImage(metadoc2, image2);
		indexer.addImage(metadoc3, image3);
		indexer.addImage(metadoc4, image4);

	}

}
