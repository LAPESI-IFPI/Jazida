package br.edu.ifpi.jazida.node.protocol;

import static br.edu.ifpi.jazida.writable.WritableUtils.convertMapWritableToMap;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.node.replication.ImageIndexReply;
import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class ImageReplicationProtocol implements IImageReplicationProtocol{
	
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public IntWritable addImageReply(MetaDocumentWritable metaDocWrapper,	BufferedImageWritable image, Text hostname, Text IP, LongWritable numDocIndex) throws KeeperException, InterruptedException, IOException {
		MetaDocument metadoc = metaDocWrapper.getMetaDoc();
		ReturnMessage result = ImageIndexReply.getTextIndexUtil().addImageReply(metadoc, image.getBufferedImage(), hostname.toString(), IP.toString(), numDocIndex.get());
		return new IntWritable(result.getCode());
	}

	@Override
	public IntWritable delImageReply(Text id, Text hostname, Text IP)
			throws KeeperException, InterruptedException, IOException {
		ReturnMessage result = ImageIndexReply.getTextIndexUtil().delImageReply(id.toString(), hostname.toString(), IP.toString());
		return new IntWritable(result.getCode());
	}

	@Override
	public IntWritable updateImageReply(Text id, MapWritable mapMetaDocument, Text hostname, Text IP)
			throws KeeperException, InterruptedException, IOException {
		Map<String, String> updates = convertMapWritableToMap(mapMetaDocument);
		ReturnMessage result = ImageIndexReply.getTextIndexUtil().updateImageReply(id.toString(), updates, hostname.toString(), IP.toString());
		return new IntWritable(result.getCode());
	}

}
