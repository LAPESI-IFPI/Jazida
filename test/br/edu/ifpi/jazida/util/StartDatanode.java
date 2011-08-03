package br.edu.ifpi.jazida.util;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Test;

import br.edu.ifpi.jazida.node.DataNode;
import br.edu.ifpi.opala.utils.Path;

public class StartDatanode {
	private DataNode datanode;
	
	@Test
	public void start() throws Exception {
		assertTrue(FileUtilsForTest.deleteDir(new File(Path.TEXT_INDEX.getValue())));
		assertTrue(FileUtilsForTest.deleteDir(new File(Path.TEXT_BACKUP.getValue())));
		assertTrue(FileUtilsForTest.deleteDir(new File(PathJazida.TEXT_INDEX_REPLY.getValue())));
		
		assertTrue(FileUtilsForTest.deleteDir(new File(Path.IMAGE_INDEX.getValue())));
		assertTrue(FileUtilsForTest.deleteDir(new File(Path.IMAGE_BACKUP.getValue())));
		assertTrue(FileUtilsForTest.deleteDir(new File(PathJazida.IMAGE_INDEX_REPLY.getValue())));
		datanode = new DataNode();
		datanode.start(true);
	}
	
	@After
	public void stop() throws InterruptedException{
		datanode.stop();
	}
	
	
}
