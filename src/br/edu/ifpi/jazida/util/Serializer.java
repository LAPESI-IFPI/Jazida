package br.edu.ifpi.jazida.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Transforma objetos em array de bytes que podem ser transferidos pela rede ou
 * salvos em disco, e podem ser reconstruídos posteriormente. Utilizada a API de
 * serialização padrão do Java.
 * 
 * @author Aécio Solano Rodrigues Santos
 * 
 */
public class Serializer {

	/**
	 * Reconstrói um objeto armazenado em um array de bytes.
	 * 
	 * @param bytes
	 *            O array de bytes contendo o objeto.
	 * @return object O objeto resconstruído.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object toObject(byte[] bytes) throws IOException,
			ClassNotFoundException {
		return Serializer.toObject(bytes, 0);
	}

	/**
	 * Reconstrói um objeto armazenado em um array de bytes.
	 * 
	 * @param bytes
	 *            O array de bytes contendo o objeto.
	 * @param start
	 *            Indica a partir que posição do array o objeto está armazenado.
	 * @return object O objeto resconstruído.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static Object toObject(byte[] bytes, int start) throws IOException,
			ClassNotFoundException {

		if (bytes == null || bytes.length == 0 || start >= bytes.length) {
			return null;
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		bais.skip(start);
		ObjectInputStream ois = new ObjectInputStream(bais);

		Object bObject = ois.readObject();

		bais.close();
		ois.close();

		return bObject;
	}

	/**
	 * Transforma um objeto recebido em um array de bytes. O objeto deve
	 * implementar a interface {@link Serializable}.
	 * 
	 * @param toBytes O objeto a ser serializado
	 * @return Um array de bytes do objeto
	 * @throws IOException
	 */
	public static byte[] fromObject(Serializable toBytes) throws IOException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);

		oos.writeObject(toBytes);
		oos.flush();

		byte[] objBytes = baos.toByteArray();

		baos.close();
		oos.close();

		return objBytes;
	}
}
