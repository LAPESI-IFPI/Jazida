package br.edu.ifpi.jazida.node.protocol;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import br.edu.ifpi.jazida.node.replication.SupportReplyImage;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.ReturneMessageJazida;
import br.edu.ifpi.jazida.writable.RestoreReplyWritable;
import br.edu.ifpi.jazida.writable.UpdateReplyWritable;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class SupportIndexImageProtocol implements ISupportIndexImageProtocol {
	
	private static final Logger LOG = Logger.getLogger(SupportIndexImageProtocol.class);
	private String HOSTNAME = DataNodeConf.DATANODE_HOSTNAME;
	
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public void loadData(Text[] fileNames, Text ipRemote, Text hostNameRemote) {
		String[] arrayFileNames = new String[fileNames.length];

		int i = 0;
		for (Text name : fileNames) {
			arrayFileNames[i] = name.toString();
			i++;
		}

		updateIndexReply(arrayFileNames, ipRemote.toString(), HOSTNAME, hostNameRemote.toString());
	}

	@Override
	public void updateIndexReply(String[] arrayFileNames, String ipRemote, String hostName, String hostNameRemote) {
		try {
			LOG.info("Atualizando réplica de imagem no " + hostNameRemote + " ...");			
			new SupportReplyImage().updateIndexReply(arrayFileNames, ipRemote, hostName, hostNameRemote);

		} catch (IOException e) {
			LOG.error("Falha no metodo: updateIndexReply() Protocol");
			LOG.error(e);
		}
	}
	
	@Override
	public IntWritable finishUpdate(UpdateReplyWritable update) {
		String hostName = update.getHostName();
		LOG.info("Atualização da réplica do " + hostName + " finalizada.");
		return new IntWritable(ReturnMessage.SUCCESS.code);
	}

	@Override
	public void restoreIndexReply(Text ipRemote, Text hostNameRemote) {
		Directory directory;
		try {
			
			LOG.info("Restaurando réplica de imagem no " + hostNameRemote + " ...");			
			directory = FSDirectory.open(new File(Path.IMAGE_BACKUP.getValue()));
			new SupportReplyImage().restoreIndexReply(directory, ipRemote.toString(), HOSTNAME.toString(), hostNameRemote.toString());

		} catch (IOException e) {
			LOG.error("Falha no metodo: restoreIndexReply() Protocol");
			LOG.error(e);
		}
				
	}
	
	@Override
	public IntWritable finishRestore(RestoreReplyWritable restore) {
		String hostName = restore.getHostName();
		LOG.info("Restautaração da réplica de imagem do " + hostName + " finalizada.");
		return new IntWritable(ReturnMessage.SUCCESS.code);
	}

	@Override
	public IntWritable checkIndexImage(IntWritable numDocsReply) {
		try{
			Directory dir = getDiretory(HOSTNAME);
			IndexReader reader = IndexReader.open(dir);
			if (numDocsReply.get() != reader.numDocs()){
				reader.close();
				return new IntWritable(ReturneMessageJazida.REPLY_OUTDATED.code);
			}		
			else {
				reader.close();
				return new IntWritable(ReturneMessageJazida.REPLY_UPDATED.code);
			}
			
			} catch (Exception e) {
				LOG.error(e);
			} catch (Throwable e) {
				LOG.error("Falha no metodo: checkIndexImage() Protocol");
			}
			return null;
		}
		
		private Directory getDiretory(String hostName) throws IOException {
			String pathDir = Path.IMAGE_INDEX.getValue();
			Directory dir = FSDirectory.open(new File(pathDir));
			return dir;
		}

}
