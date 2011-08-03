package br.edu.ifpi.jazida.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.jazida.util.FileUtilsForTest;
import br.edu.ifpi.opala.indexing.ImageIndexer;
import br.edu.ifpi.opala.utils.Conversor;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.MetaDocumentBuilder;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class ImageIndexerClientTest {
	
	private static DataNode datanode;
	private static final File IMAGE_01 = new File("./sample-data/images/image01.bmp");
	private static final File IMAGE_02 = new File("./sample-data/images/image02.bmp");
	
	@BeforeClass
	public static void setUpTest() throws Exception {
		FileUtilsForTest.deleteDir(new File(Path.IMAGE_INDEX.getValue()));
		FileUtilsForTest.deleteDir(new File("./data/backup/image"));
		datanode = new DataNode();
		datanode.start(false);
		
	}
	
	@AfterClass
	public static void tearDownTest() throws InterruptedException {
		datanode.stop();
	}

	
	@Test
	public final void deveriaDevolverSuccessQuandoIndexarDuasImagens() throws IOException, KeeperException, InterruptedException {
		//dado
		MetaDocument metadoc = new MetaDocumentBuilder()
										.id("imagem01.bmp")
										.keywords("anoitecer por-do-sol")
										.title("Por do sol")
										.build();
		
		MetaDocument metadoc2 = new MetaDocumentBuilder()
										.id("imagem02.bmp")
										.keywords("anoitecer por-do-sol")
										.title("Por do sol")
										.build();
		
		ImageIndexer indexer = new ImageIndexerClient();
		
		BufferedImage image = Conversor.byteArrayToBufferedImage(
									Conversor.fileToByteArray(IMAGE_01));
		BufferedImage image2 = Conversor.byteArrayToBufferedImage(
				Conversor.fileToByteArray(IMAGE_02));
		
		//quando
		ReturnMessage returnMessage = indexer.addImage(metadoc, image);
		ReturnMessage returnMessage2 = indexer.addImage(metadoc2, image2);
		
		//então
		assertThat(returnMessage, is(equalTo(ReturnMessage.SUCCESS)));
		assertThat(returnMessage2, is(equalTo(ReturnMessage.SUCCESS)));
	}
	
}
