package br.edu.ifpi.jazida.node.protocol;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import br.edu.ifpi.jazida.node.replication.SupportReplyText;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.ReturneMessageJazida;
import br.edu.ifpi.jazida.writable.RestoreReplyWritable;
import br.edu.ifpi.jazida.writable.UpdateReplyWritable;
import br.edu.ifpi.opala.utils.Path;

public class SupportReplyTextProtocol implements ISupportReplyTextProtocol {
	
	private static final Logger LOG = Logger.getLogger(SupportReplyTextProtocol.class);
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
			new SupportReplyText().updateIndexReply(arrayFileNames, ipRemote, hostName, hostNameRemote);
		} catch (IOException e) {
			LOG.error("Falha no metodo: updateIndexReply() Protocol");
			LOG.error(e.fillInStackTrace(), e);
		}
	}
	
	@Override
	public void finishUpdate(UpdateReplyWritable update) {
		String hostName = update.getHostName();
		LOG.info("Atualização da réplica de texto do " + hostName+ " finalizada.");
	}

	@Override
	public void restoreIndexReply(Text ipRemote, Text hostNameRemote) {
		Directory directory;
		try {
			directory = FSDirectory.open(new File(Path.TEXT_BACKUP.getValue()));
			new SupportReplyText().restoreIndexReply(directory, ipRemote.toString(), HOSTNAME.toString(), hostNameRemote.toString());

		} catch (IOException e) {
			LOG.error("Falha no metodo: restoreIndexReply() Protocol");
			LOG.error(e.fillInStackTrace(), e);
		}
				
	}
	
	@Override
	public void finishRestore(RestoreReplyWritable restore) {
		String hostName = restore.getHostName();
		LOG.info("Uma nova réplica de texto para o " + hostName+ " foi criada.");
	}

	@Override
	public IntWritable checkIndexText(IntWritable numDocsReply) {
		try{
		Directory dir = getDiretory();
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
			LOG.error("Falha no metodo: checkIndexText() Protocol");
			LOG.error(e.fillInStackTrace(), e);
		}
		return null;
	}
	
	private Directory getDiretory() throws IOException {
		String pathDir = Path.TEXT_INDEX.getValue();
		Directory dir = FSDirectory.open(new File(pathDir));
		return dir;
	}
}
