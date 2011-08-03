package br.edu.ifpi.jazida.node.protocol;

import static br.edu.ifpi.jazida.writable.WritableUtils.convertMapWritableToMap;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.node.replication.TextReplicationNode;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.indexing.TextIndexer;
import br.edu.ifpi.opala.statistic.Statistic;
import br.edu.ifpi.opala.utils.MetaDocument;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class TextIndexerProtocol implements ITextIndexerProtocol {

	private TextIndexer textIndexer;

	public TextIndexerProtocol(TextIndexer indexer) {
		this.textIndexer = indexer;
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public IntWritable addText(MetaDocumentWritable metaDocWrapper, Text content) {
		MetaDocument metadoc = metaDocWrapper.getMetaDoc();
		ReturnMessage result = textIndexer.addText(metadoc, content.toString());

		if ((result.equals(ReturnMessage.SUCCESS)) || (result.equals(ReturnMessage.OUTDATED))
				|| (result.equals(ReturnMessage.UNEXPECTED_BACKUP_ERROR))) {
			
			try {
				TextReplicationNode.getTextReplicationNodeUtil().addTextReply(metaDocWrapper, content, new LongWritable(Statistic.numTextDocs()));
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return new IntWritable(result.getCode());
	}

	@Override
	public IntWritable delText(Text identifier) {
		ReturnMessage result = null;
		try {
			result = textIndexer.delText(identifier.toString());
			
			if (result.equals(ReturnMessage.SUCCESS) || result.equals(ReturnMessage.OUTDATED)
				|| result.equals(ReturnMessage.UNEXPECTED_BACKUP_ERROR)) {
				try {
					new TextReplicationNode().delTextReply(identifier);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			return new IntWritable(result.getCode());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	@Override
	public IntWritable updateText(Text id, MapWritable updatesWritable) {
		Map<String, String> updates = convertMapWritableToMap(updatesWritable);
		ReturnMessage result;
		try {
			result = textIndexer.updateText(id.toString(), updates);
			
			if (result.equals(ReturnMessage.SUCCESS) || result.equals(ReturnMessage.OUTDATED)
				|| result.equals(ReturnMessage.UNEXPECTED_BACKUP_ERROR)) {
				
				try {
					new TextReplicationNode().updateTextReply(id, updatesWritable);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			return new IntWritable(result.getCode());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}
}
