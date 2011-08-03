package br.edu.ifpi.jazida.loadtest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface LoadTestProtocol extends VersionedProtocol {
	long versionID = 0;

	/**
	 * Executa testes de busca em um servidor remoto.
	 * 
	 * @param totalQueries Quantidade de queries a serem executadas
	 * @param totalThreads Quantidade de Threads a serem usadas para as buscas
	 * @return Tempo total de execução das buscas.
	 */
	public LongWritable executeSearchLoadTest(	IntWritable totalQueries,
												IntWritable totalThreads);
	
	/**
	 * Executa um cliente de indexação em um servidor remoto
	 * @param totalDocuments Quantidade de documentos a serem indexados
	 * @param totalThreads Quantidade de Threads que seerão usadas para 
	 * indexar os documentos
	 * 
	 * @return Tempo total de indexação dos documentos
	 */
	public LongWritable executeIndexingLoadTest(Text fileName, 
												IntWritable totalDocuments,
												IntWritable totalThreads);
}