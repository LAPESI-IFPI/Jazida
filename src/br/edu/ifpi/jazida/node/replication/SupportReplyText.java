package br.edu.ifpi.jazida.node.replication;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

import br.edu.ifpi.jazida.node.NodeStatus;
import br.edu.ifpi.jazida.node.protocol.ISupportReplyTextProtocol;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.PathJazida;
import br.edu.ifpi.jazida.util.ReturneMessageJazida;
import br.edu.ifpi.jazida.writable.RestoreReplyWritable;
import br.edu.ifpi.jazida.writable.UpdateReplyWritable;
import br.edu.ifpi.opala.utils.Path;

public class SupportReplyText {
	
	private static final Logger LOG = Logger.getLogger(SupportReplyText.class);
	private final String IP_LOCAL = DataNodeConf.DATANODE_HOSTADDRESS;
	private final int PORTA = DataNodeConf.TEXT_REPLICATION_SUPPORT_SERVER_PORT;
	private final String PATH_INDEX = Path.TEXT_BACKUP.getValue();
	private final String PATH_REPLY = PathJazida.TEXT_INDEX_REPLY.getValue();
	private Configuration HADOOP_CONFIGURATION = new Configuration();
	
	public SupportReplyText(){
	}		
		
	private ISupportReplyTextProtocol getSupportIndexTextServer(final InetSocketAddress address) {
		try{
			ISupportReplyTextProtocol proxy = (ISupportReplyTextProtocol) RPC.getProxy(
													ISupportReplyTextProtocol.class,
													ISupportReplyTextProtocol.versionID,
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
			ISupportReplyTextProtocol supportProxy = getSupportIndexTextServer(new InetSocketAddress(ipRemote, PORTA));
			supportProxy.loadData(fileNames, new Text(IP_LOCAL), new Text(hostNameLocal));
		}catch (Throwable e){
			LOG.error("Falha no metodo: startUpdateIndexReply()");
			LOG.error(e.fillInStackTrace(), e);
		}
	}
	
	
	public void updateIndexReply(String[] fileNames, String ipRemote, String hostName, String hostNameRemote) throws IOException {
		try{
			UpdateReplyWritable update = new UpdateReplyWritable(fileNames, hostName, PATH_INDEX, PATH_REPLY);
			ISupportReplyTextProtocol supportProxy = getSupportIndexTextServer(new InetSocketAddress(ipRemote, PORTA));
			
			supportProxy.finishUpdate(update);
		}catch (Throwable e){
			LOG.error("Falha no metodo: updateIndexReply()");
			LOG.error(e.fillInStackTrace(), e);
		}
	}
	
	public void startRestoreIndexReply(String ipRemote, String hostNameLocal) throws IOException {
		try{
			ISupportReplyTextProtocol supportProxy = getSupportIndexTextServer(new InetSocketAddress(ipRemote, PORTA));
			supportProxy.restoreIndexReply(new Text(IP_LOCAL), new Text(hostNameLocal));
		}catch (Throwable e){
			LOG.error("Falha no metodo: startRestoreIndexReply()");
			LOG.error(e.fillInStackTrace(), e);
		}
	}	
	
	public void restoreIndexReply(Directory dir, String ipRemote, String hostName, String hostNameRemote) throws IOException {
		try{
			RestoreReplyWritable restore = new RestoreReplyWritable(dir, hostName, PATH_REPLY);
			ISupportReplyTextProtocol supportProxy = getSupportIndexTextServer(new InetSocketAddress(ipRemote, PORTA));
		
			supportProxy.finishRestore(restore);
		}catch (Throwable e){
			LOG.error("Falha no metodo: restoreIndexReply()");
			LOG.error(e.fillInStackTrace(), e);
		}
	}
	
	public void checkRepliesText(List<NodeStatus> nodesReplyReceive){
		LOG.info("Verificando atualização das réplicas de texto...");
		try{
			final Map<String, ISupportReplyTextProtocol> supportProxy = new HashMap<String, ISupportReplyTextProtocol>();
			ExecutorService threadPool  = Executors.newCachedThreadPool();
			
			for (NodeStatus node : nodesReplyReceive) {
				final InetSocketAddress socketAdress = new InetSocketAddress(node.getAddress(), PORTA);
				ISupportReplyTextProtocol supportReplyTextProtocol = getSupportIndexTextServer(socketAdress);
				
				if(supportReplyTextProtocol != null){
					supportProxy.put(node.getHostname(), supportReplyTextProtocol);	
				}
			}
			
			for (final NodeStatus nodeStatus : nodesReplyReceive) {
				if(getDiretory(nodeStatus.getHostname()) != null){
					Future<IntWritable> request = threadPool.submit(new Callable<IntWritable>() {
						@Override
						public IntWritable call() throws Exception {
							IndexReader reader = IndexReader.open(getDiretory(nodeStatus.getHostname()));
							int numDocs = reader.numDocs();
							reader.close();
							
							ISupportReplyTextProtocol proxy = supportProxy.get(nodeStatus.getHostname());
							return proxy.checkIndexText(new IntWritable(numDocs));
						}
					});
					
					IntWritable returnCode = request.get(3500, TimeUnit.MILLISECONDS);
					if(ReturneMessageJazida.getReturnMessage(returnCode.get()) == ReturneMessageJazida.REPLY_OUTDATED){
						LOG.info("Atualizando réplica de texto do "+ nodeStatus.getHostname() + "...");
						new SupportReplyText().startUpdateIndexReply(nodeStatus.getAddress(), getDiretory(nodeStatus.getHostname()),
								DataNodeConf.DATANODE_HOSTNAME);
					} else if (ReturneMessageJazida.getReturnMessage(returnCode.get()) == ReturneMessageJazida.REPLY_UPDATED){
						LOG.info("Réplica de texto do "+ nodeStatus.getHostname() + " atualizada.");
					}
					
				} else {
					String pathDir = PathJazida.TEXT_INDEX_REPLY.getValue();
					createIndexIfNotExists(new File(pathDir + "/" + nodeStatus.getHostname()));
					LOG.info("Restaurando réplica de texto do "+ nodeStatus.getHostname() + "...");
					new SupportReplyText().startRestoreIndexReply(nodeStatus.getAddress(), DataNodeConf.DATANODE_HOSTNAME);
				}
			}
			
			} catch (ConcurrentModificationException e) {
				LOG.info("Reordenando listas.");
			} catch (InterruptedException e) {
				LOG.error(e.fillInStackTrace(), e);
			} catch (ExecutionException e) {
				LOG.info("Reordenando listas.");
			} catch (TimeoutException e) {
				LOG.error(e.fillInStackTrace(), e);
			}catch (Throwable e) {
				LOG.error("Falha no metodo: checkRepliesText()");
				LOG.error(e.fillInStackTrace(), e);
			}	
	}
	
	private Directory getDiretory(String hostName) throws IOException {
		String pathDir = PathJazida.TEXT_INDEX_REPLY.getValue();
		Directory dir = null;
		
		if(new File(pathDir + "/" + hostName).canRead())
			dir = FSDirectory.open(new File(pathDir + "/" + hostName));
		
		return dir;
	}
	
	private void createIndexIfNotExists(File indexPath)
	throws CorruptIndexException, LockObtainFailedException, IOException {

		IndexWriter indexWriter = new IndexWriter(	FSDirectory.open(indexPath),
													new BrazilianAnalyzer(Version.LUCENE_30),
													IndexWriter.MaxFieldLength.UNLIMITED);
		indexWriter.close();
	}

}
