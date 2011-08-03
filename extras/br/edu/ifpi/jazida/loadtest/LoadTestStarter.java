package br.edu.ifpi.jazida.loadtest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

public class LoadTestStarter {
	
	private static final Configuration HADOOP_CONFIGURATION = new Configuration();
	private static IntWritable queriesPerHost;
	private static IntWritable searchThreads;
	private static IntWritable docsPerHost;
	private static IntWritable indexingThreads;
	private static Text fileName;
	private static String servers;
	
	public static void main(String[] args) throws Exception {
		String[] arguments = {"search"};
		loadProperties();
		if(args.length > 0) {
			arguments = args;
		}
		if("search".equals(arguments[0]))
			buscas(parseServers(servers));
		else if("index".equals(arguments[0]))
			indexacoes(parseServers(servers));
		
		System.exit(0);
	}
	
	public static void loadProperties() throws IOException {
		Properties p = new Properties();
		p.load(new FileInputStream(new File("conf/jazida.loadtest.properties")));
		queriesPerHost = new IntWritable(Integer.parseInt(p.getProperty("loadtest.search.queries")));
		searchThreads = new IntWritable(Integer.parseInt(p.getProperty("loadtest.search.threads")));
		docsPerHost = new IntWritable(Integer.parseInt(p.getProperty("loadtest.indexing.docs")));
		indexingThreads = new IntWritable(Integer.parseInt(p.getProperty("loadtest.indexing.threads")));
		fileName = new Text(p.getProperty("loadtest.indexing.filename"));
		servers = p.getProperty("loadtest.servers");
	}
	
	public static List<InetSocketAddress> parseServers(String properties){
		StringTokenizer socketAddressTokenizer = new StringTokenizer(properties, ",");
		List<InetSocketAddress> hosts = new ArrayList<InetSocketAddress>();
		while (socketAddressTokenizer.hasMoreElements()) {
			StringTokenizer ipTokenizer = new StringTokenizer(socketAddressTokenizer.nextToken(), ":");
			hosts.add(new InetSocketAddress(ipTokenizer.nextToken(),
											Integer.parseInt(ipTokenizer.nextToken())));
		}
		return hosts;
	}

	private static void indexacoes(List<InetSocketAddress> hosts) throws Exception {
		
		List<LoadTestProtocol> proxies = new ArrayList<LoadTestProtocol>();
		System.out.println("Iniciando requisições de indexação em:");
		for (int i=0; i < hosts.size(); i++) {
			proxies.add(getLoadTestProxy(hosts.get(i)));
			System.out.println(i +" => "+hosts.get(i).getAddress());
		}
		ExecutorService executor = Executors.newCachedThreadPool();
		List<Future<Long>> futures = new ArrayList<Future<Long>>();
		System.out.println("Iniciando tempo...");
		long inicio = System.currentTimeMillis();
		for (final LoadTestProtocol proxy : proxies) {
			futures.add(
				executor.submit(new Callable<Long>() {
					@Override
					public Long call() throws Exception {
						LongWritable tempo = proxy.executeIndexingLoadTest(fileName, docsPerHost, indexingThreads);
						return tempo.get(); 
						
					}
				})
			);
		}
		for (Future<Long> future : futures) {
			future.get();
		}
		long tempoEmMs = System.currentTimeMillis() - inicio;
		double docs = (hosts.size())*docsPerHost.get();
		
		System.out.println("Tempo total: "+tempoEmMs+" ms");
		System.out.println("Nº de indexações: "+docs);
		System.out.println(docs/(tempoEmMs/1000.0));		
	}

	private static void buscas(List<InetSocketAddress> hosts) throws Exception {
		List<LoadTestProtocol> proxies = new ArrayList<LoadTestProtocol>();
		System.out.println("Iniciando requisições de busca em:");
		for (int i=0; i < hosts.size(); i++) {
			proxies.add(getLoadTestProxy(hosts.get(i)));
			System.out.println(i +" => "+hosts.get(i).getAddress());
		}
		ExecutorService executor = Executors.newCachedThreadPool();
		List<Future<Long>> futures = new ArrayList<Future<Long>>();
		System.out.println("Iniciando tempo...");
		long inicio = System.currentTimeMillis();
		for (final LoadTestProtocol proxy : proxies) {
			futures.add(
				executor.submit(new Callable<Long>() {
					@Override
					public Long call() throws Exception {
						return proxy.executeSearchLoadTest(queriesPerHost, searchThreads).get();
					}
				})
			);
		}
		for (Future<Long> future : futures) {
			future.get();
		}
		long tempoEmMs = System.currentTimeMillis() - inicio;
		double queries = (hosts.size())*queriesPerHost.get();
		
		System.out.println("Tempo total: "+tempoEmMs+" ms");
		System.out.println("Nº de Queries: "+queries);
		System.out.println(queries/(tempoEmMs/1000.0));
	}

	private static LoadTestProtocol getLoadTestProxy(final InetSocketAddress endereco) 
	throws IOException {
		LoadTestProtocol proxy = (LoadTestProtocol) RPC.getProxy(
											LoadTestProtocol.class,
											LoadTestProtocol.versionID,
											endereco,
											HADOOP_CONFIGURATION);
		return proxy;
	}

}
