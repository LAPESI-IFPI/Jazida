package br.edu.ifpi.jazida.extras;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

import br.edu.ifpi.jazida.node.protocol.ITextIndexerProtocol;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.MetaDocument;

class CircularTextIndexer {


	static String[] servers = { "monica-desktop", "monica-desktop",
								"mario-desktop", "mario-desktop",
								"luigi-desktop", "luigi-desktop"};
	static Thread[] threads;
	
	public static void main(String[] args) throws IOException, InterruptedException {

		long inicio = System.currentTimeMillis();

		Configuration conf = new Configuration();
		threads = new Thread[servers.length];
		for (int i = 0; i < servers.length; i++) {
			String host = servers[i];
			InetSocketAddress addr = new InetSocketAddress(host, 16000);
			System.out.println("Criando  cliente para "+servers[i]);
			ITextIndexerProtocol opalaClient = (ITextIndexerProtocol) RPC.getProxy (
					ITextIndexerProtocol.class, ITextIndexerProtocol.versionID, addr, conf);
			
			Thread indexer = new Thread((Runnable) new IndexerThread(i, opalaClient));
			threads[i] = indexer;
			threads[i].start();
		}
		
		IndexerThread.awaitForComplete();
		
		long fim = System.currentTimeMillis();
		long total = fim - inicio;

		System.out.println("\nTempo:" + total / 1000.0);
	}
}

class IndexerThread implements Runnable {
	
	private static CountDownLatch connectedSignal;
	private ITextIndexerProtocol client;
	private int threadId;
	private static int runningThreads = 0;
	private static File pasta = new File("/home/yoshi/dados-teste/mil-txt");
	private static File[] arquivos = pasta.listFiles();
	private static int atual = -1;
	
	public IndexerThread(int threadId, ITextIndexerProtocol client) {
		this.client = client;
		this.threadId = threadId;
		runningThreads++;
	}
	
	public static void awaitForComplete() throws InterruptedException{
		connectedSignal = new CountDownLatch(runningThreads);
		connectedSignal.await();
	}
	
	
	@Override
	public void run() {
		File arquivo = nextDocument();
		while (arquivo != null) {
			try {
				MetaDocument metadoc = new MetaDocument();
				metadoc.setTitle(arquivo.getName());
				metadoc.setId(arquivo.getName());
				
				// Ler conteúdo do arquivo
				FileInputStream stream;
				stream = new FileInputStream(arquivo);
				InputStreamReader streamReader = new InputStreamReader(
						stream);
				BufferedReader reader = new BufferedReader(streamReader);
				
				StringBuffer stringBuffer = new StringBuffer();
				String line;
				while ((line = reader.readLine()) != null) {
					stringBuffer.append(line);
				}
				reader.close();
				streamReader.close();
				stream.close();
				
				// Indexar documento
				IntWritable code = client.addText(
						new MetaDocumentWritable(metadoc),
						new Text(stringBuffer.toString()) );
				
				System.out.println("Thread #"+threadId+" - "+code);
				
			} catch (Exception e) {
				System.out.println("Erro ao indexer arquivo...");
			}
			arquivo = nextDocument();
		}
		
		if( connectedSignal != null ){
			connectedSignal.countDown();
		} else {
			runningThreads--;
		}
	}
	
	private synchronized static File nextDocument() {
		atual++;
		if (atual < arquivos.length) {
			System.out.println("Arquivo: "+atual+"/"+arquivos.length);
			return arquivos[atual];
		} else {
			return null;
		}
	}
	
}