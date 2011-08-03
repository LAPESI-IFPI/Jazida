package br.edu.ifpi.jazida.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import br.edu.ifpi.jazida.util.Serializer;

public abstract class AbstractWritable implements Writable {
	
	private static final Logger LOG = Logger.getLogger(AbstractWritable.class); 
	private static final long serialVersionUID = 1L;
	
	private Serializable object;
	
	public AbstractWritable(){
		//Necessário para serialização
	}
	
	public AbstractWritable(Serializable obj){
		this.object = obj;
	}
	
	protected Serializable getObject() {
		if(object instanceof Exception)
			return null;
		else
			return object;
	}
	
	public boolean isError() {
		if(object instanceof Exception)
			return true;
		else
			return false;
	}
	
	public Exception getException() {
		if(object instanceof Exception)
			return (Exception) object;
		else
			return null;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		byte[] objectBytes = new byte[size];
		for (int i = 0; i < size; i++) {
			objectBytes[i] = in.readByte();
		}
		try {
			object = (Serializable) Serializer.toObject(objectBytes, 0);
		} catch (ClassNotFoundException e) {
			LOG.error("Erro ao reconstruir o objeto serializado!");
			throw new RuntimeException("Erro ao reconstruir o objeto!");
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		byte[] objectBytes = Serializer.fromObject(object);
		int size = objectBytes.length;

		out.writeInt(size);
		out.write(objectBytes);
	}

}
