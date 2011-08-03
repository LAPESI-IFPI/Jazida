package br.edu.ifpi.jazida.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;

public class FileUtilsForTest {
	/**
	 * Lê o conteúdo do arquivo passado como paramêtro.
	 * 
	 * @param arquivo Um arquivo de texto
	 * @return Texto contido no arquivo
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static String conteudoDoArquivo(File arquivo)
	throws FileNotFoundException, IOException {
		
		InputStreamReader isr = new InputStreamReader(new FileInputStream(arquivo));
		BufferedReader reader = new BufferedReader(isr);
		StringBuffer stringBuffer = new StringBuffer();
		String line = null;
		while ((line = reader.readLine()) != null) {
			stringBuffer.append(line);
		}
		reader.close();
		isr.close();
		
		return stringBuffer.toString();
	}
	
	/**
	 * Copia uma diretório de índice de srcFile para destFile.
	 * 
	 * @param srcFile
	 * @param destFile
	 * @throws IOException
	 */
	public static void copyIndex(File srcFile, File destFile) throws IOException{
		Directory srcDir = new SimpleFSDirectory(srcFile);
		Directory destDir = new SimpleFSDirectory(destFile);
		Directory.copy(srcDir, destDir, true);
	}

	/**
	 * Apaga o arquivo ou o diretório informado recursivamente.
	 * 
	 * @param dir Arquivo ou diretório a ser apagado.
	 * @return falso se não conseguiu apagar ao menos um arquivo
	 */
	public static synchronized boolean deleteDir(File dir) {
		if (!dir.exists())
			return true;
		if (dir.isDirectory()) {
			File[] children = dir.listFiles();
			for (File child : children) {
				boolean success = deleteDir(child);
				if (!success) {
					return false;
				}
			}
		}
		return dir.delete();
	}
}
