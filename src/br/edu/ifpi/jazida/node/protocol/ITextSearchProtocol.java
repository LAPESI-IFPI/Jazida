package br.edu.ifpi.jazida.node.protocol;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import br.edu.ifpi.jazida.writable.SearchResultWritable;

@Deprecated
public interface ITextSearchProtocol extends VersionedProtocol {
	
	public static final long versionID = 0;
	
	public SearchResultWritable search( MapWritable fields,
										Text[] returnedFields, 
										IntWritable batchStart, 
										IntWritable batchSize,
										Text sortOn, 
										BooleanWritable reverse);
	
	public SearchResultWritable search( MapWritable fields,
										Text[] returnedFields, 
										IntWritable batchStart, 
										IntWritable batchSize,
										Text sortOn); 
}
