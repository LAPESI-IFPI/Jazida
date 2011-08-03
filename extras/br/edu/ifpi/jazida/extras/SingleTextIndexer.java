package br.edu.ifpi.jazida.extras;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

import br.edu.ifpi.jazida.node.protocol.ITextIndexerProtocol;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.MetaDocument;

public class SingleTextIndexer {

	private static File pasta = new File("/home/yoshi/dados-teste/mil-txt");
	private static File[] arquivos = pasta.listFiles();
	private static int atual = -1;

	public static void main(String[] args) throws IOException {

		long inicio = System.currentTimeMillis();

		Configuration conf = new Configuration();
		InetSocketAddress addr = new InetSocketAddress("monica-desktop", 16000);

		ITextIndexerProtocol client = (ITextIndexerProtocol) RPC.waitForProxy(
				ITextIndexerProtocol.class, ITextIndexerProtocol.versionID, addr,
				conf);

		File arquivo = nextDocument();
		while (arquivo != null) {

			MetaDocument metadoc = new MetaDocument();
			metadoc.setTitle(arquivo.getName());
			metadoc.setId(arquivo.getName());

			// Ler conteúdo do arquivo
			FileInputStream stream;
			stream = new FileInputStream(arquivo);
			InputStreamReader streamReader = new InputStreamReader(stream);
			BufferedReader reader = new BufferedReader(streamReader);

			StringBuffer stringBuffer = new StringBuffer();
			String line;
			while ((line = reader.readLine()) != null) {
				stringBuffer.append(line);
			}

			// Indexar documento
			IntWritable code = client.addText(new MetaDocumentWritable(metadoc),
					new Text(stringBuffer.toString()));

			System.out.println(code);
			arquivo = nextDocument();
		}

		long fim = System.currentTimeMillis();
		long total = fim - inicio;

		System.out.println("Tempo:" + total / 1000.0);
	}

	private static File nextDocument() {
		atual++;
		if (atual < arquivos.length) {
			return arquivos[atual];
		} else {
			return null;
		}
	}
}
