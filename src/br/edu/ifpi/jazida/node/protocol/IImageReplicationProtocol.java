package br.edu.ifpi.jazida.node.protocol;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;

public interface IImageReplicationProtocol extends VersionedProtocol {
	
	long versionID = 0;
	
	public IntWritable addImageReply(MetaDocumentWritable metaDocWrapper, BufferedImageWritable image, Text hostname, Text IP, LongWritable numDocIndex) throws KeeperException, InterruptedException, IOException;
	public IntWritable delImageReply(Text id, Text hostname, Text IP)throws KeeperException, InterruptedException, IOException;
	public IntWritable updateImageReply(Text id, MapWritable mapMetaDocument, Text hostname, Text IP)throws KeeperException, InterruptedException, IOException;
}





