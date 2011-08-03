package br.edu.ifpi.jazida.extras;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.zookeeper.KeeperException;

import br.edu.ifpi.jazida.client.TextSearcherClient;
import br.edu.ifpi.opala.searching.SearchResult;
import br.edu.ifpi.opala.utils.Metadata;
import br.edu.ifpi.opala.utils.QueryMapBuilder;

public class SearchPerformanceTest {

	private static final File QUERIES_FILE = new File("./sample-data/queries.txt");
	private static int MAX_QUERIES = 1000;

//	private static int MAX_QUERIES = 22064; //Quantidade de linhas do arquivo queries.txt
	private static int THREADS = 100;

	private TextSearcherClient searcher;
	
	public static void main(String[] args) throws Exception {
		if(args.length > 0) {
			try {
				MAX_QUERIES = Integer.parseInt(args[0]);
				new SearchPerformanceTest().start(MAX_QUERIES, THREADS);
			}
			catch (NumberFormatException e) {
				System.out.println("O parâmetro deve ser um inteiro indicando a " +
								"quantidade máxima de Queries a serem executadas.");
			}
		}else {
			new SearchPerformanceTest().start(MAX_QUERIES, THREADS);
		}
		
		System.exit(1);
	}
	
	public SearchPerformanceTest() throws Exception  {
		searcher = new TextSearcherClient();
	}

	public long start(int maxQueries, int threads) throws InterruptedException, ExecutionException, IOException, KeeperException {
		
		QueryFileReader reader = new QueryFileReader(QUERIES_FILE, maxQueries);
		ExecutorService executor = Executors.newFixedThreadPool(threads);

		List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
		long inicio = System.currentTimeMillis();
		for(int i=0;i<threads;i++) {
			Future<Integer> future = executor.submit(new QueriesRunner(searcher, reader));
			futures.add(future);
		}
		int totalQueries = 0;
		for(Future<Integer> result: futures) {
			totalQueries += result.get();
		}
		long tempoTotal = System.currentTimeMillis() - inicio;
		
		System.out.println("Tempo de execução: "+tempoTotal+" ms");
		System.out.println("Queries executadas: "+totalQueries);
		System.out.println("Queries por segundo: "+ (totalQueries/(tempoTotal/1000.0))+" QPS");
		System.out.println("Tempo médio por Query: "+ (tempoTotal/totalQueries)+" ms");
		
		searcher.close();
		
		return tempoTotal;
	}
		
	private class QueriesRunner implements Callable<Integer> {
		private final QueryFileReader reader;
		private final TextSearcherClient searcher;
		private int queriesCount = 0;

		public QueriesRunner(TextSearcherClient searcher, QueryFileReader reader) {
			this.searcher = searcher;
			this.reader = reader;
		}
		
		@Override
		public Integer call(){
			try {
				String word;
				while((word = reader.nextQuery())!=null) {
					Map<String, String> query = new QueryMapBuilder()
														.content(word)
														.build();
					
					List<String> returnedFields = new ArrayList<String>();
					returnedFields.add(Metadata.AUTHOR.getValue());
					returnedFields.add(Metadata.TITLE.getValue());
					returnedFields.add(Metadata.PUBLICATION_DATE.getValue());
					returnedFields.add(Metadata.CONTENT.getValue());

					SearchResult searchResult = searcher.search(query, returnedFields , 1, 10, null);
					queriesCount++;
					System.out.println(searchResult.getCodigo() +" hits para busca por "+ word);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return queriesCount;
		}
	}

	private class QueryFileReader {
		private final BufferedReader reader;
		private final int maxLines;
		private int i;

		public QueryFileReader(File file, int maxLines) throws IOException {
			this.reader = new BufferedReader(new FileReader(file));
			this.maxLines = maxLines;
		}
		
		public synchronized String nextQuery() throws IOException {
			if( i < maxLines ) { 
				i++;
				return reader.readLine();
			} else {
				return null;
			}
		}
	}
}
