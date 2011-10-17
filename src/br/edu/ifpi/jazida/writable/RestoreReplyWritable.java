package br.edu.ifpi.jazida.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import br.edu.ifpi.opala.utils.IndexManager;

public class RestoreReplyWritable implements Writable {
	private static final int BUFFER_SIZE = 65536;
	private Directory directory;
	private String hostName;
	private String pathReply;
	private String[] fileNames;

	public RestoreReplyWritable() {
	}

	public RestoreReplyWritable(Directory directory, String hostName, String pathReply){
		this.directory = directory;
		this.hostName = hostName;
		this.pathReply = pathReply;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int sizeFileArray = in.readInt();
		hostName = in.readUTF();
		pathReply = in.readUTF();
		
		directory = FSDirectory.open(new File(pathReply + "/" + hostName));

		deleteFiles(directory);

		fileNames = new String[sizeFileArray];
		for (int i = 0; i < sizeFileArray; i++) {
			fileNames[i] = in.readUTF();
		}


		for (String fileName : fileNames) {
			IndexOutput io = null;
			byte[] buf = null;
			long len = 0;
			long readCount = 0;
			int toRead = 0;
			try {

				io = directory.createOutput(fileName);
				len = in.readLong();
				buf = new byte[BUFFER_SIZE];
				while (readCount < len) {
					toRead = in.readInt();
					for (int cont = 0; cont < BUFFER_SIZE; cont++) {
						buf[cont] = in.readByte();
					}
					io.writeBytes(buf, toRead);
					readCount = in.readLong();
				}

			} finally {
				if (io != null)
					io.close();
			}
		}

		directory.close();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int size = directory.listAll().length;
		out.writeInt(size);
		out.writeUTF(hostName);
		out.writeUTF(pathReply);
		
		for (String fileName : directory.listAll()) {
			out.writeUTF(fileName);
		}
		
		IndexManager.snapshotterBackup();

		for (String fileName : directory.listAll()) {
			IndexInput is = null;
			byte[] buf = null;
			long len = 0;
			try {

				is = directory.openInput(fileName);
				len = is.length();
				out.writeLong(len);
				buf = new byte[BUFFER_SIZE];

				long readCount = 0;
				while (readCount < len) {
					int toRead = (int) (readCount + BUFFER_SIZE > len ? (int) (len - readCount)
							: BUFFER_SIZE);
					out.writeInt(toRead);
					is.readBytes(buf, 0, toRead);
					out.write(buf);
					readCount += toRead;
					out.writeLong(readCount);
				}

			} finally {
				if (is != null)
					is.close();
			}
		}
		
		IndexManager.releaseSnapshotterBackup();
	}

	public void deleteFiles(Directory directory) {
		
		try{
			for (String name : directory.listAll()) {
				if(directory.fileExists(name))
					directory.deleteFile(name);
			}
		} catch(IOException e){
			return;
		}

	}

	public String getHostName() {
		return hostName;
	}
}
