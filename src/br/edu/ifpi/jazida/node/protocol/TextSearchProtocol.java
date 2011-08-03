package br.edu.ifpi.jazida.node.protocol;

import static br.edu.ifpi.jazida.writable.WritableUtils.*;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import br.edu.ifpi.jazida.writable.SearchResultWritable;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.searching.TextSearcherImpl;
@Deprecated
public class TextSearchProtocol implements ITextSearchProtocol {
	
	private Logger LOG = Logger.getLogger(TextSearchProtocol.class);

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public SearchResultWritable search( MapWritable fields,
										Text[] returnedFields, 
										IntWritable batchStart, 
										IntWritable batchSize,
										Text sortOn, 
										BooleanWritable reverse) {
		LOG.info("Iniciando busca no DataNode");
		
		SearchResult result = new TextSearcherImpl().search(convertMapWritableToMap(fields),
															convertTextArrayToStringList(returnedFields),
															batchStart.get(), 
															batchSize.get(), 
															sortOn.toString(),
															reverse.get());
		LOG.info("Busca finalizada com CÓDIGO: "+ result.getCodigo());
		return new SearchResultWritable(result);
	}

	@Override
	public SearchResultWritable search( MapWritable fields,
										Text[] returnedFields, 
										IntWritable batchStart, 
										IntWritable batchSize,
										Text sortOn) {
		
		SearchResult result = new TextSearcherImpl().search(convertMapWritableToMap(fields),
															convertTextArrayToStringList(returnedFields),
															batchStart.get(), 
															batchSize.get(), 
															sortOn.toString());

		return new SearchResultWritable(result);
	}

}
