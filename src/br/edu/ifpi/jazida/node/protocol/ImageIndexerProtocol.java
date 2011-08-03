package br.edu.ifpi.jazida.node.protocol;

import static br.edu.ifpi.jazida.writable.WritableUtils.convertMapWritableToMap;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.node.replication.ImageReplicationNode;
import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.indexing.ImageIndexerImpl;
import br.edu.ifpi.opala.statistic.Statistic;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class ImageIndexerProtocol implements IImageIndexerProtocol {

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public IntWritable addImage(MetaDocumentWritable metaDocWrapper,
								BufferedImageWritable image) {
		ReturnMessage message = ImageIndexerImpl.getImageIndexerImpl().addImage(metaDocWrapper.getMetaDoc(), image.getBufferedImage());
		
		if (message.equals(ReturnMessage.SUCCESS) || message.equals(ReturnMessage.OUTDATED)
				|| message.equals(ReturnMessage.UNEXPECTED_BACKUP_ERROR)) {
			
			try {
				ImageReplicationNode.getImageReplicationNodeUtil().addImageReply(metaDocWrapper, image, new LongWritable(Statistic.numImageDocs()));
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return new IntWritable(message.getCode());
	}

	@Override
	public IntWritable delImage(Text id) {
		ReturnMessage message = ImageIndexerImpl.getImageIndexerImpl().delImage(id.toString());
		
		if (message.equals(ReturnMessage.SUCCESS) || message.equals(ReturnMessage.OUTDATED)
				|| message.equals(ReturnMessage.UNEXPECTED_BACKUP_ERROR)) {
			
			try {
				new ImageReplicationNode().delImageReply(id);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return new IntWritable(message.getCode());
	}

	@Override
	public IntWritable updateImage(Text id, MapWritable mapMetaDocument) {
		Map<String, String> updates = convertMapWritableToMap(mapMetaDocument);
		ReturnMessage message = ImageIndexerImpl.getImageIndexerImpl().updateImage(id.toString(), updates);
		
		if (message.equals(ReturnMessage.SUCCESS) || message.equals(ReturnMessage.OUTDATED)
				|| message.equals(ReturnMessage.UNEXPECTED_BACKUP_ERROR)) {
			
			try {
				new ImageReplicationNode().updateImageReply(id, mapMetaDocument);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}		
		
		return new IntWritable(message.getCode());
	}

}
