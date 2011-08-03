package br.edu.ifpi.jazida.node.protocol;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import br.edu.ifpi.jazida.writable.RestoreReplyWritable;
import br.edu.ifpi.jazida.writable.UpdateReplyWritable;

public interface ISupportIndexTextProtocol extends VersionedProtocol{
	public static final long versionID = 0;
	
	public void loadData(Text[] fileNames, Text IP_REMOTE, Text HOSTNAME_REMOTE);
	public void updateIndexReply(String[] arrayFileNames, String IP_REMOTE, String hostname, String HOSTNAME_REMOTE);
	public IntWritable finishUpdate(UpdateReplyWritable update);
	public void restoreIndexReply(Text IP_REMOTE, Text HOSTNAME_REMOTE);
	public IntWritable finishRestore(RestoreReplyWritable restore);
}