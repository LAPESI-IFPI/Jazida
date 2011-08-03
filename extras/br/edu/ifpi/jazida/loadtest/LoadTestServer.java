package br.edu.ifpi.jazida.loadtest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import br.edu.ifpi.jazida.extras.SearchPerformanceTest;
import br.edu.ifpi.jazida.extras.WikipediaFileIndexer;
import br.edu.ifpi.jazida.node.RPCServer;

public class LoadTestServer implements LoadTestProtocol {	
	
	private static RPCServer loadTestServer;
	private static int SERVER_PORT = 29000;
	private static String IP_ADDRESS = "localhost";

	public static void main(String[] args) throws Exception {
		if(args.length > 0) {
			new LoadTestServer().start(args[0], Integer.parseInt(args[1]));
		}else {
			Properties p = new Properties();
			p.load(new FileInputStream(new File("conf/jazida.loadtest.properties")));
			IP_ADDRESS = p.getProperty("loadtest.server.address");
			SERVER_PORT = Integer.parseInt(p.getProperty("loadtest.server.port"));
			new LoadTestServer().start(IP_ADDRESS, SERVER_PORT);
		}
	}

	private SearchPerformanceTest searchTest;
	private WikipediaFileIndexer indexingTest;
	

	private void start(String IPAddress, int serverPort) throws Exception {
		searchTest = new SearchPerformanceTest();
		indexingTest = new WikipediaFileIndexer();
		loadTestServer = new RPCServer(	this,
										IPAddress,
										serverPort);
		loadTestServer.start(true);
	}


	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return 0;
	}

	@Override
	public LongWritable executeSearchLoadTest(	IntWritable totalQueries, 
												IntWritable totalThreads) {
		
		System.out.println("LoadTestServer.executeSearchLoadTest("+totalQueries.get()+","+totalThreads.get()+")");
		LongWritable tempo = null;
		try {
			tempo = new LongWritable(searchTest.start(totalQueries.get(), totalThreads.get()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tempo;
	}

	@Override
	public LongWritable executeIndexingLoadTest(Text fileName,
												IntWritable totalDocuments,
												IntWritable totalThreads) {
		LongWritable tempo = null;
		try {
			tempo = new LongWritable(indexingTest.start(new File(fileName.toString()), 
														totalDocuments.get(),
														totalThreads.get()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tempo;
	}

}
