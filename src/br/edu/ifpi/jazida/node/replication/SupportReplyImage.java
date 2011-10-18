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
import br.edu.ifpi.jazida.node.protocol.ISupportReplyImageProtocol;
import br.edu.ifpi.jazida.util.DataNodeConf;
import br.edu.ifpi.jazida.util.PathJazida;
import br.edu.ifpi.jazida.util.ReturneMessageJazida;
import br.edu.ifpi.jazida.writable.RestoreReplyWritable;
import br.edu.ifpi.jazida.writable.UpdateReplyWritable;
import br.edu.ifpi.opala.utils.Path;

public class SupportReplyImage {
	
	private static final Logger LOG = Logger.getLogger(SupportReplyImage.class);
	private final String IP_LOCAL = DataNodeConf.DATANODE_HOSTADDRESS;
	private final int PORTA = DataNodeConf.IMAGE_REPLICATION_SUPPORT_SERVER_PORT;
	private final String HOSTNAME = DataNodeConf.DATANODE_HOSTNAME;
	private final String PATH_INDEX = Path.IMAGE_BACKUP.getValue();
	private final String PATH_REPLY = PathJazida.IMAGE_INDEX_REPLY.getValue();
	private Configuration HADOOP_CONFIGURATION = new Configuration();
	
	public SupportReplyImage(){
	}		
		
	private ISupportReplyImageProtocol getSupportIndexImageProtocol(final InetSocketAddress address) {
		try{
			ISupportReplyImageProtocol proxy = (ISupportReplyImageProtocol) RPC.getProxy(
												ISupportReplyImageProtocol.class,
												ISupportReplyImageProtocol.versionID,
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
			ISupportReplyImageProtocol supportProxy = getSupportIndexImageProtocol(new InetSocketAddress(ipRemote, PORTA));
			supportProxy.loadData(fileNames, new Text(IP_LOCAL), new Text(hostNameLocal));
		}catch (Throwable e){
			LOG.error("Falha no metodo: startUpdateIndexReply()");
			LOG.error(e.fillInStackTrace(), e);
		}
	}
	
	
	public void updateIndexReply(String[] fileNames, String ipRemote, String hostName, String hostNameRemote) throws IOException {
		try{
			UpdateReplyWritable update = new UpdateReplyWritable(fileNames, hostName, PATH_INDEX, PATH_REPLY);
			ISupportReplyImageProtocol supportProxy = getSupportIndexImageProtocol(new InetSocketAddress(ipRemote, PORTA));
		
			supportProxy.finishUpdate(update);
		}catch (Throwable e){
			LOG.error("Falha no metodo: updateIndexReply()");
			LOG.error(e.fillInStackTrace(), e);
		}
	}
	
	public void startRestoreIndexReply(String ipRemote, String hostNameLocal) throws IOException {
		try{
			ISupportReplyImageProtocol supportProxy = getSupportIndexImageProtocol(new InetSocketAddress(ipRemote, PORTA));
			supportProxy.restoreIndexReply(new Text(IP_LOCAL), new Text(hostNameLocal));
		}catch (Throwable e){
			LOG.error("Falha no metodo: startRestoreIndexReply()");
			LOG.error(e.fillInStackTrace(), e);
		}
	}
	
	
	public void restoreIndexReply(Directory dir, String ipRemote, String hostName, String hostNameRemote) throws IOException {
		try{
			RestoreReplyWritable restore = new RestoreReplyWritable(dir, hostName, PATH_REPLY);
			ISupportReplyImageProtocol supportProxy = getSupportIndexImageProtocol(new InetSocketAddress(ipRemote, PORTA));
		
			supportProxy.finishRestore(restore);
		}catch (Throwable e){
			LOG.error("Falha no metodo: restoreIndexReply()");
			LOG.error(e.fillInStackTrace(), e);
		}
	}

	public void checkRepliesImage(List<NodeStatus> nodesReplyReceive) {
		LOG.info("Verificando atualização das réplicas de imagem...");
		try{
			final Map<String, ISupportReplyImageProtocol> supportProxy = new HashMap<String, ISupportReplyImageProtocol>();
			ExecutorService threadPool  = Executors.newCachedThreadPool();
			
			for (NodeStatus node : nodesReplyReceive) {
				final InetSocketAddress socketAdress = new InetSocketAddress(node.getAddress(), PORTA);
				ISupportReplyImageProtocol supportReplyImageProtocol = getSupportIndexImageProtocol(socketAdress);
				
				if(supportReplyImageProtocol != null){
					supportProxy.put(node.getHostname(), supportReplyImageProtocol);	
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
							
							ISupportReplyImageProtocol proxy = supportProxy.get(nodeStatus.getHostname());
							return proxy.checkIndexImage(new IntWritable(numDocs));
						}
					});
					
					IntWritable returnCode = request.get(3500, TimeUnit.MILLISECONDS);
					if(ReturneMessageJazida.getReturnMessage(returnCode.get()) == ReturneMessageJazida.REPLY_OUTDATED){
						LOG.info("Atualizando réplica de imagem do "+ nodeStatus.getHostname() + "...");
						new SupportReplyImage().startUpdateIndexReply(nodeStatus.getAddress(), 
																	getDiretory(nodeStatus.getHostname()), HOSTNAME);
						
					}else if(ReturneMessageJazida.getReturnMessage(returnCode.get()) == ReturneMessageJazida.REPLY_UPDATED){
						LOG.info("Réplica de imagem do "+ nodeStatus.getHostname() + " atualizada.");
					}			
					
				} else {
					String pathDir = PathJazida.IMAGE_INDEX_REPLY.getValue();
					createIndexIfNotExists(new File(pathDir + "/" + nodeStatus.getHostname()));
					LOG.info("Restaurando réplica de imagem do "+ nodeStatus.getHostname() + "...");
					new SupportReplyImage().startRestoreIndexReply(nodeStatus.getAddress(), HOSTNAME);
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
			} catch (Throwable e){
				LOG.error("Falha no metodo: checkRepliesImage()");
				LOG.error(e.fillInStackTrace(), e);
			}
	}
	
	private Directory getDiretory(String hostName) throws IOException {
		Directory dir = null;
		
		if(new File(PATH_REPLY + "/" + hostName).canRead())
			dir = FSDirectory.open(new File(PATH_REPLY + "/" + hostName));
		
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
