package br.edu.ifpi.jazida.node.protocol;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.writable.MetaDocumentWritable;

public interface ITextReplicationProtocol extends VersionedProtocol {

	long versionID = 0;

	public IntWritable addTextReply(MetaDocumentWritable metaDocument,
			Text content, Text hostname, Text IP, LongWritable numDocsIndex) throws KeeperException,
			InterruptedException, IOException;

	public IntWritable delTextReply(Text identifier, Text hostname, Text IP) throws KeeperException,
	InterruptedException, IOException;

	public IntWritable updateTextReply(Text identifier,	MapWritable updatesWritable, Text hostname, Text IP)throws KeeperException,
			InterruptedException, IOException;

}
