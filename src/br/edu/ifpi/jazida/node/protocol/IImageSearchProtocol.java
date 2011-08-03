package br.edu.ifpi.jazida.node.protocol;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.SearchResultWritable;

public interface IImageSearchProtocol extends VersionedProtocol {
	
	long versionID = 0;

	public SearchResultWritable search(BufferedImageWritable image, IntWritable limit);
	
//	public SearchResult search(	Map<String, String> fields,
//								List<String> returnedFields,
//								int batchStart,
//								int batchSize,
//								String sortOn,
//								boolean reverse);
//	
//	public SearchResult search(	Map<String, String> fields,
//								List<String> returnedFields,
//								int batchStart,
//								int batchSize,
//								String sortOn);

}
