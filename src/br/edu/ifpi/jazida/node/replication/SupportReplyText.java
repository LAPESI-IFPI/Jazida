package br.edu.ifpi.jazida.node.replication;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.lucene.store.Directory;

import br.edu.ifpi.jazida.node.protocol.ISupportIndexTextProtocol;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.PathJazida;
import br.edu.ifpi.jazida.writable.RestoreReplyWritable;
import br.edu.ifpi.jazida.writable.UpdateReplyWritable;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class SupportReplyText {
	
	private static final Logger LOG = Logger.getLogger(SupportReplyText.class);
	private final String IP_LOCAL = DataNodeConf.DATANODE_HOSTADDRESS;
	private final int PORTA = DataNodeConf.TEXT_REPLICATION_SUPPORT_SERVER_PORT;
	private final String PATH_INDEX = Path.TEXT_BACKUP.getValue();
	private final String PATH_REPLY = PathJazida.TEXT_INDEX_REPLY.getValue();
	private Configuration HADOOP_CONFIGURATION = new Configuration();
	
	public SupportReplyText(){
	}		
		
	private ISupportIndexTextProtocol getSupportIndexTextServer(final InetSocketAddress address) {
		try{
			ISupportIndexTextProtocol proxy = (ISupportIndexTextProtocol) RPC.getProxy(
													ISupportIndexTextProtocol.class,
													ISupportIndexTextProtocol.versionID,
													address, HADOOP_CONFIGURATION);
			return proxy;
		}catch(IOException e){
			LOG.error("Erro ao criar o proxy.");
		}
		return null;
	}
	
	public void startUpdateIndexReply(String IP_REMOTE, Directory directory, String HOSTNAME_LOCAL) throws IOException{
		int size = directory.listAll().length;
		String[] array = new String[size];
		Text[] fileNames = new Text[size];
		array = directory.listAll();		
		
		int i=0;
		for(String fileName: array){
			fileNames[i] = new Text(fileName);
			i++;
		}		
		
		try{
			ISupportIndexTextProtocol supportProxy = getSupportIndexTextServer(new InetSocketAddress(IP_REMOTE, PORTA));
			supportProxy.loadData(fileNames, new Text(IP_LOCAL), new Text(HOSTNAME_LOCAL));
		}catch (Throwable e){
				LOG.error(e);
		}
	}
	
	
	public void updateIndexReply(String[] fileNames, String IP_REMOTE, String HOSTNAME, String HOSTNAME_REMOTE) throws IOException {
		try{
			UpdateReplyWritable update = new UpdateReplyWritable(fileNames, HOSTNAME, PATH_INDEX, PATH_REPLY);
			ISupportIndexTextProtocol supportProxy = getSupportIndexTextServer(new InetSocketAddress(IP_REMOTE, PORTA));
			
			IntWritable result = supportProxy.finishUpdate(update);
			LOG.info("A atualização da réplica no "+ HOSTNAME_REMOTE + " retornou: " +ReturnMessage.getReturnMessage(result.get()));
		}catch (Throwable e){
			LOG.error(e);
		}
	}
	
	public void startRestoreIndexReply(String IP_REMOTE, String HOSTNAME_LOCAL) throws IOException {
		try{
			ISupportIndexTextProtocol supportProxy = getSupportIndexTextServer(new InetSocketAddress(IP_REMOTE, PORTA));
			supportProxy.restoreIndexReply(new Text(IP_LOCAL), new Text(HOSTNAME_LOCAL));
		}catch (Throwable e){
			LOG.error(e);
		}
	}
	
	
	public void restoreIndexReply(Directory dir, String IP_REMOTE, String HOSTNAME, String HOSTNAME_REMOTE) throws IOException {
		try{
			RestoreReplyWritable restore = new RestoreReplyWritable(dir, HOSTNAME, PATH_REPLY);
			ISupportIndexTextProtocol supportProxy = getSupportIndexTextServer(new InetSocketAddress(IP_REMOTE, PORTA));
		
			IntWritable result = supportProxy.finishRestore(restore);
			LOG.info("A restauração da réplica no "+ HOSTNAME_REMOTE + " retornou: " +ReturnMessage.getReturnMessage(result.get()));
		}catch (Throwable e){
			LOG.error(e);
		}
	}
	
}
