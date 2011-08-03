package br.edu.ifpi.jazida.extras;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import br.edu.ifpi.jazida.client.TextIndexerClient;
import br.edu.ifpi.jazida.extras.WikipediaFileReader.WikiDocument;

public class WikipediaFileIndexer {

	private static File fileName = new File("./sample-data/wikipedia.lines.txt");
	private static int numDocs = 5000;
	private static int threads = 100;

	private int totalDocs = 0;
	private TextIndexerClient indexer;
	private WikipediaFileReader wikiFile;

	public static void main(String[] args) throws Exception {
		if(args.length>0) {
			fileName = new File(args[0]);
			numDocs = Integer.parseInt(args[1]);
			threads = Integer.parseInt(args[2]);
			new WikipediaFileIndexer().start(fileName, numDocs, threads);
			
		}else {
			new WikipediaFileIndexer().start(fileName, numDocs, threads);
		}
		System.exit(1);
	}
	
	public WikipediaFileIndexer() throws Exception { 
		indexer = new TextIndexerClient();
	}
	
	public long start(File fileName, int maxLines, int threads)
	throws IOException, InterruptedException, ExecutionException {
		
		wikiFile = new WikipediaFileReader(fileName);
		ExecutorService executor = Executors.newCachedThreadPool();
		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
		
		long inicio = System.currentTimeMillis();
		for(int i=0;i<threads;i++) {
			Future<Integer> future = executor.submit(new IndexerTask());
			futures.add(future);
		}
		for(Future<Integer> result: futures) {
			result.get();
		}
		long tempoDeExecucao = (System.currentTimeMillis() - inicio);
		
		System.out.println(totalDocs+" documentos indexados em "+tempoDeExecucao+" ms");
		System.out.println("Througput: "+(totalDocs/(tempoDeExecucao/1000.0))+" docs/seg");
		
		return tempoDeExecucao;
	}

	public synchronized WikiDocument next() {
		try {
			if(totalDocs < numDocs ) {
				totalDocs++;
				return wikiFile.nextDocument();
			}
			else {
				return null;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	class IndexerTask implements Callable<Integer>{
		@Override
		public Integer call() throws Exception {
			WikiDocument doc;
			while((doc = next()) != null) {
				indexer.addText(doc.getMetadoc(), doc.getContent());
			}
			return null;
		}
		
	}
}
