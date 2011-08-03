package br.edu.ifpi.jazida.node.protocol;

import static br.edu.ifpi.jazida.writable.WritableUtils.convertMapWritableToMap;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.node.replication.TextIndexReply;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class TextReplicationProtocol implements ITextReplicationProtocol {

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}
	
	@Override
	public IntWritable addTextReply(MetaDocumentWritable metaDocWrapper, Text content, Text hostname, Text IP, LongWritable numDocsIndex) throws KeeperException, InterruptedException, IOException {
		MetaDocument metadoc = metaDocWrapper.getMetaDoc();
		ReturnMessage result = TextIndexReply.getTextIndexUtil().addTextReply(metadoc, content.toString(), hostname.toString(), IP.toString(), numDocsIndex.get());
		return new IntWritable(result.getCode());
	}

	@Override
	public IntWritable delTextReply(Text identifier, Text hostname, Text IP)	throws KeeperException, InterruptedException, IOException {
		ReturnMessage result = TextIndexReply.getTextIndexUtil().delTextReply(identifier.toString().trim(), hostname.toString(), IP.toString());
		return new IntWritable(result.getCode());
	}	

	@Override
	public IntWritable updateTextReply(Text identifier,	MapWritable updatesWritable, Text hostname, Text IP) throws KeeperException,	InterruptedException, IOException {
		Map<String, String> updates = convertMapWritableToMap(updatesWritable);		
		ReturnMessage result = TextIndexReply.getTextIndexUtil().updateTextReply(identifier.toString().trim(), updates, hostname.toString(), IP.toString());
		return new IntWritable(result.getCode());
	}

}
