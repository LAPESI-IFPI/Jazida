package br.edu.ifpi.jazida.node.protocol;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import br.edu.ifpi.jazida.writable.BufferedImageWritable;
import br.edu.ifpi.jazida.writable.MetaDocumentWritable;
import br.edu.ifpi.opala.utils.ReturnMessage;

public interface IImageIndexerProtocol extends VersionedProtocol {
	
	long versionID = 0;

	/** 
	 * Assinatura para o método que realiza a adição da imagem no índice.
	 */
	public IntWritable addImage(MetaDocumentWritable metaDocument, BufferedImageWritable image);
	
	/**
	 * Assinatura para o método que remove a imagem do índice.
	 * 
	 */
	public IntWritable delImage(Text id);

	/**
	 * Assinatura para o método que atualiza uma imagem no índice
	 * @return {@link ReturnMessage#SUCCESS} se a imagem foi atualizada
	 */
	public IntWritable updateImage(Text id, MapWritable metaDocument);

}
