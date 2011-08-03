package br.edu.ifpi.jazida.node;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;

import br.edu.ifpi.jazida.node.protocol.TextSearchProtocol;
import br.edu.ifpi.jazida.util.FileUtilsForTest;
import br.edu.ifpi.jazida.util.UtilForTest;
import br.edu.ifpi.jazida.writable.SearchResultWritable;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.ReturnMessage;

@Deprecated
public class TextSearchProtocolTest {

	private static final String SAMPLE_DATA_FOLDER = "./sample-data/texts";

	@BeforeClass
	public static void setUpTest() throws Exception {
		FileUtilsForTest.deleteDir(new File(Path.TEXT_INDEX.getValue()));
		assertTrue(UtilForTest.indexTextDirOrFile(new File(SAMPLE_DATA_FOLDER)));
	}

	@Test
	public final void deveriaEncontarUmDocumentoQueJaFoiIndexado() throws Exception {
		//dado
		MapWritable fields = new MapWritable();
		fields.put(new Text(Metadata.CONTENT.getValue()), new Text("Alice"));
		
		Text[] returnedFields = {new Text(Metadata.AUTHOR.getValue()), new Text(Metadata.TITLE.getValue())};
		
		TextSearchProtocol searcher = new TextSearchProtocol();
		
		//quando
		SearchResultWritable resultado = searcher.search(fields,
														returnedFields,
														new IntWritable(1),
														new IntWritable(10),
														new Text(Metadata.ID.getValue()));

		//então
		assertThat(resultado.getSearchResult().getCodigo(), is(equalTo(ReturnMessage.SUCCESS)));
		assertThat(resultado.getSearchResult().getItems().size(), is(equalTo(1)));
	}
	
}
