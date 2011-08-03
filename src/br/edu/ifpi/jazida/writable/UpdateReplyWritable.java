package br.edu.ifpi.jazida.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import br.edu.ifpi.opala.utils.IndexManager;

public class UpdateReplyWritable implements Writable {
	private final int BUFFER_SIZE = 65536;
	
	private Directory directory;
	private String hostName;
	private String[] fileNames;
	private String pathReply;
	private List<String> listUpdade = new ArrayList<String>();

	public UpdateReplyWritable() throws IOException {
	}

	public UpdateReplyWritable(String[] fileNames, String hostname, String pathIndex, String pathReply)
			throws IOException {
		this.fileNames = fileNames;
		this.hostName = hostname;
		this.pathReply = pathReply;
		directory = FSDirectory.open(new File(pathIndex));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		int sizeFileArray = in.readInt();
		hostName = in.readUTF();
		pathReply = in.readUTF();

		directory = FSDirectory.open(new File(pathReply + "/" + hostName));

		fileNames = new String[sizeFileArray];
		for (int i = 0; i < sizeFileArray; i++) {
			fileNames[i] = in.readUTF();
		}

		updateReply(fileNames);			
		
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
					io.flush();
					readCount = in.readLong();
				}

			} finally {
				if (io != null)
					io.close();
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		loadFileNamesUpdate(fileNames);
		int size = listUpdade.size();
		out.writeInt(size);
		out.writeUTF(hostName);
		out.writeUTF(pathReply);

		for (String fileName : listUpdade) {
			out.writeUTF(fileName);
		}
		
		try{
			IndexManager.snapshotterBackup();
			
			for (String fileName : listUpdade) {
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
			
		} finally {
			IndexManager.releaseSnapshotterBackup();
		}
		
				
	}

	public List<String> loadFileNamesUpdate(String[] fileNames)
			throws IOException {

		int i = 0;
		boolean exists = false;
		for (String fileNameIndex : directory.listAll()) {
			
			if(!fileNameIndex.equals("write.lock")){
				if (fileNameIndex.equals("segments.gen")){
					listUpdade.add(fileNameIndex);
					i++;
				} 
				
				else {	
					for (String fileNameReply : fileNames) {
						if (fileNameIndex.equals(fileNameReply)){
							exists = true;
							break;
						}
					}
				
	
					if (exists == false) {
						listUpdade.add(fileNameIndex);
						i++;
					}
				}
			}
			
			exists = false;
		}

		return listUpdade;
	}

	public void updateReply(String[] arrayFileNamesIndex) throws IOException {
		String fileDelete = "";

		boolean exists = false;
		for (String fileIndex : arrayFileNamesIndex) {			
			
			if (fileIndex.equals("segments.gen")){
				fileDelete = fileIndex;
			}
			
			else{
				for (String fileReply : directory.listAll()) {
					
					fileDelete = fileReply;
					if (fileIndex.equals(fileReply)){
						exists = true;
						break;
					}
	
				}
			}

			if ((exists == false)) {
				directory.deleteFile(fileDelete);
			}

			exists = false;
			
			if(directory.listAll().length == 0){
				return;
			}
		}		
	}

	public String getHostName() {
		return hostName;
	}

}
