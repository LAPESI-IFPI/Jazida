package br.edu.ifpi.jazida.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;


public class IndexImageWritable implements Writable {
	private static final Logger LOG = Logger.getLogger(IndexImageWritable.class);
	File sourceIndex;
	File[] files = null;
	List<String> namesList = new ArrayList<String>();
	
	public IndexImageWritable() {
	}

	public IndexImageWritable(File sourceIndex) {
		this.sourceIndex = sourceIndex;
		this.files = sourceIndex.listFiles();
	}

	public File getReplyWritable() {
		return sourceIndex;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		int sizeFileArray = in.readInt();
		
		File replyDist = new File("./data/replica/imagem");
		String fileName;
		File file = null;
		FileOutputStream out = null;
		
		for (int i = 0; i < sizeFileArray; i++) {
			namesList.add(in.readUTF());
		}
		
		for (int i = 0; i < sizeFileArray; i++) {

			fileName = namesList.get(i).replace('/', File.separatorChar);
			fileName = namesList.get(i).replace('\\', File.separatorChar);
			file = new File(replyDist, fileName);
			
			if (replyDist.isDirectory()) {  
				final File parent = file.getParentFile();  
	               if (parent != null) {  
	                  parent.mkdirs();  
	               }  
	               file.createNewFile(); 
	         } 
			
			else{ 
				if(!file.exists()) {  
			}
	               final File parent = file.getParentFile();  
	               if (parent != null) {  
	                  parent.mkdirs();  
	               }  
	               file.createNewFile();  
	            }
			out = new FileOutputStream(file);
			int sizeFile = in.readInt();
			byte[] buf = new byte[sizeFile];
			for (int cont = 0; cont < sizeFile; cont++) {
				buf[cont] = in.readByte();				
			}
			
			out.write(buf);

		}
		
		out.close();
		sourceIndex = replyDist;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		int size = files.length;
		out.writeInt(size);

		for (File file : files) {
			String fileName = file.getName();
			out.writeUTF(fileName);
		}

		for (File file : files) {
			
			FileInputStream input = new FileInputStream(file);

			byte[] buf = new byte[input.available()];
			int sizeFile = (int) file.length();
			int offset = 0;

			input.read(buf, offset, sizeFile);
			out.writeInt(sizeFile);
			out.write(buf);
			
			input.close();
		}
		
		LOG.info("Indice de imagem enviado.");

	}

}
