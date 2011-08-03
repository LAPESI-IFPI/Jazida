package br.edu.ifpi.jazida.node.protocol;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import br.edu.ifpi.jazida.node.replication.SupportReplyImage;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.writable.RestoreReplyWritable;
import br.edu.ifpi.jazida.writable.UpdateReplyWritable;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class SupportIndexImageProtocol implements ISupportIndexImageProtocol {
	
	private static final Logger LOG = Logger.getLogger(SupportReplyImage.class);
	private String HOSTNAME = DataNodeConf.DATANODE_HOSTNAME;
	
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public void loadData(Text[] fileNames, Text IP_REMOTE, Text HOSTNAME_REMOTE) {
		String[] arrayFileNames = new String[fileNames.length];

		int i = 0;
		for (Text name : fileNames) {
			arrayFileNames[i] = name.toString();
			i++;
		}

		updateIndexReply(arrayFileNames, IP_REMOTE.toString(), HOSTNAME, HOSTNAME_REMOTE.toString());
	}

	@Override
	public void updateIndexReply(String[] arrayFileNames, String IP_REMOTE, String HOSTNAME, String HOSTNAME_REMOTE) {
		try {
			LOG.info("Atualizando réplica de imagem no " + HOSTNAME_REMOTE + " ...");			
			new SupportReplyImage().updateIndexReply(arrayFileNames, IP_REMOTE, HOSTNAME, HOSTNAME_REMOTE);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public IntWritable finishUpdate(UpdateReplyWritable update) {
		String hostName = update.getHostName();
		LOG.info("Atualização da réplica do " + hostName + " finalizada.");
		return new IntWritable(ReturnMessage.SUCCESS.code);
	}

	@Override
	public void restoreIndexReply(Text IP_REMOTE, Text HOSTNAME_REMOTE) {
		Directory directory;
		try {
			
			LOG.info("Restaurando réplica de imagem no " + HOSTNAME_REMOTE + " ...");			
			directory = FSDirectory.open(new File(Path.IMAGE_BACKUP.getValue()));
			new SupportReplyImage().restoreIndexReply(directory, IP_REMOTE.toString(), HOSTNAME.toString(), HOSTNAME_REMOTE.toString());

		} catch (IOException e) {
			e.printStackTrace();
		}
				
	}
	
	@Override
	public IntWritable finishRestore(RestoreReplyWritable restore) {
		String hostName = restore.getHostName();
		LOG.info("Restautaração da réplica do " + hostName + " finalizada.");
		return new IntWritable(ReturnMessage.SUCCESS.code);
	}

}
