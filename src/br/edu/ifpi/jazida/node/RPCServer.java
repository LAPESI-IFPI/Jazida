package br.edu.ifpi.jazida.node;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.VersionedProtocol;

public class RPCServer {
	
	private static final Configuration HADOOP_CONF = new Configuration();
	private Server server;

	/**
	 * Receber o nome do servidor e a porta em que irar escutar como parâmetros.
	 * 
	 * @param serverName
	 *            O nome do servidor RPC
	 * @param port
	 *            A port que o servidor receberá chamadas RPC
	 * @throws IOException
	 */
	public RPCServer(VersionedProtocol protocol, String serverName, int port) throws IOException {
		server = RPC.getServer(protocol, serverName, port, HADOOP_CONF);
	}

	/**
	 * Inicia o servidor RPC no host e porta passados no construtor. Se o 
	 * paramêtro join for igual a true, a Thread ficará bloqueada até que o 
	 * processo seja interropido. Se for false, o servidor será iniciado e a 
	 * execução continuará normalmente.
	 * 
	 * @param join
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void start(boolean join) throws IOException, InterruptedException {
		server.start();
		if (join) {
			server.join();
		}
	}

	public void stop() {
		server.stop();
	}
}
