package br.edu.ifpi.jazida.writable;

import java.awt.image.BufferedImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import br.edu.ifpi.opala.utils.Conversor;

public class BufferedImageWritable implements Writable {

	BufferedImage image;
		
	public BufferedImageWritable() {
	}

	public BufferedImageWritable(BufferedImage image) {
		this.image = image;
	}
	
	public BufferedImage getBufferedImage() {
		return image;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		byte[] objectBytes = new byte[size];
		for (int i = 0; i < size; i++) {
			objectBytes[i] = in.readByte();
		}
		image = Conversor.byteArrayToBufferedImage(objectBytes);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		byte[] objectBytes = Conversor.BufferdImageToByteArray(image);
		int size = objectBytes.length;
		out.writeInt(size);
		out.write(objectBytes);
	}

}
