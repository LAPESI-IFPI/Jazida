package br.edu.ifpi.jazida.node.protocol;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import br.edu.ifpi.jazida.writable.RestoreReplyWritable;
import br.edu.ifpi.jazida.writable.UpdateReplyWritable;

public interface ISupportReplyImageProtocol extends VersionedProtocol{
	public static final long versionID = 0;
	
	public void loadData(Text[] fileNames, Text ipRemote, Text hostNameRemote);
	public void updateIndexReply(String[] arrayFileNames, String ipRemote, String hostname, String hostNameRemote);
	public void finishUpdate(UpdateReplyWritable update);
	public void restoreIndexReply(Text ipRemote, Text hostNameRemote);
	public void finishRestore(RestoreReplyWritable restore);
	public IntWritable checkIndexImage(IntWritable numDocsReply);
}