package br.edu.ifpi.jazida.node.replication;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.lucene.store.Directory;

import br.edu.ifpi.jazida.node.protocol.ISupportIndexImageProtocol;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.PathJazida;
import br.edu.ifpi.jazida.writable.RestoreReplyWritable;
import br.edu.ifpi.jazida.writable.UpdateReplyWritable;
import br.edu.ifpi.opala.utils.Path;
import br.edu.ifpi.opala.utils.ReturnMessage;

public class SupportReplyImage {
	
	private static final Logger LOG = Logger.getLogger(SupportReplyImage.class);
	private final String IP_LOCAL = DataNodeConf.DATANODE_HOSTADDRESS;
	private final int PORTA = DataNodeConf.IMAGE_REPLICATION_SUPPORT_SERVER_PORT;
	private final String PATH_INDEX = Path.IMAGE_BACKUP.getValue();
	private final String PATH_REPLY = PathJazida.IMAGE_INDEX_REPLY.getValue();
	private Configuration HADOOP_CONFIGURATION = new Configuration();
	
	public SupportReplyImage(){
	}		
		
	private ISupportIndexImageProtocol getSupportIndexImageProtocol(final InetSocketAddress address) {
		try{
			ISupportIndexImageProtocol proxy = (ISupportIndexImageProtocol) RPC.getProxy(
												ISupportIndexImageProtocol.class,
												ISupportIndexImageProtocol.versionID,
													address, HADOOP_CONFIGURATION);
			return proxy;
		}catch(IOException e){
			LOG.error("Erro ao criar o proxy.");
		}
		return null;
	}
	
	public void startUpdateIndexReply(String ipRemote, Directory directory, String hostNameLocal) throws IOException{
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
			ISupportIndexImageProtocol supportProxy = getSupportIndexImageProtocol(new InetSocketAddress(ipRemote, PORTA));
			supportProxy.loadData(fileNames, new Text(IP_LOCAL), new Text(hostNameLocal));
		}catch (Throwable e){
			LOG.error(e);
		}
	}
	
	
	public void updateIndexReply(String[] fileNames, String ipRemote, String hostName, String hostNameRemote) throws IOException {
		try{
			UpdateReplyWritable update = new UpdateReplyWritable(fileNames, hostName, PATH_INDEX, PATH_REPLY);
			ISupportIndexImageProtocol supportProxy = getSupportIndexImageProtocol(new InetSocketAddress(ipRemote, PORTA));
		
			IntWritable result = supportProxy.finishUpdate(update);
			LOG.info("A atualização da réplica no "+ hostNameRemote + " retornou: " +ReturnMessage.getReturnMessage(result.get()));
		}catch (Throwable e){
			LOG.error(e);
		}
	}
	
	public void startRestoreIndexReply(String ipRemote, String hostNameLocal) throws IOException {
		try{
			ISupportIndexImageProtocol supportProxy = getSupportIndexImageProtocol(new InetSocketAddress(ipRemote, PORTA));
			supportProxy.restoreIndexReply(new Text(IP_LOCAL), new Text(hostNameLocal));
		}catch (Throwable e){
			LOG.error(e);
		}
	}
	
	
	public void restoreIndexReply(Directory dir, String ipRemote, String hostName, String hostNameRemote) throws IOException {
		try{
			RestoreReplyWritable restore = new RestoreReplyWritable(dir, hostName, PATH_REPLY);
			ISupportIndexImageProtocol supportProxy = getSupportIndexImageProtocol(new InetSocketAddress(ipRemote, PORTA));
		
			IntWritable result = supportProxy.finishRestore(restore);
			LOG.info("A restauração da réplica no "+ hostNameRemote + " retornou: " +ReturnMessage.getReturnMessage(result.get()));
		}catch (Throwable e){
			LOG.error(e);
		}
	}
	
}
