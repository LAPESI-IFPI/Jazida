package br.edu.ifpi.jazida.node.protocol;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.ReturnMessage;

/**
 * Interface dos métodos de indexação de texto do Servidor IPC/RPC do Jazida.
 * @author Aécio Santos
 *
 */
public interface ITextIndexerProtocol extends VersionedProtocol {

	public static final long versionID = 0;

	/**
	 * Assinatura para o método que realiza a adição da Texto no índice.
	 * 
	 * @param metaDocument  Os metadados associados ao texto.
	 * @param content O conteúdo do texto a ser indexado
	 * @return status Código de sucesso ou erro de acordo com a Enum {@link ReturnMessage}
	 */
	public IntWritable addText(MetaDocumentWritable metaDocument, Text content);
	
	/**
	 * Assinatura para o método que remove o texto do índice.
	 * 
	 * @param identifier - O identificador único do texto no índice.            
	 * @return status Código de sucesso ou erro de acordo com a Enum {@link ReturnMessage}
	 * 
	 */
	public IntWritable delText(Text identifier);
	
	/**
	 * Assinatura para o método que atualiza um documento no índice
	 * @return status Código de sucesso ou erro de acordo com a Enum {@link ReturnMessage}
	 */
	public IntWritable updateText(Text id, MapWritable metaDocumentMap);
}
