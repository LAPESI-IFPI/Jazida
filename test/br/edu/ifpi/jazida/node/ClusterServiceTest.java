package br.edu.ifpi.jazida.node;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import br.edu.ifpi.jazida.util.ListsManager;
import br.edu.ifpi.jazida.util.Serializer;
import br.edu.ifpi.jazida.util.ZkConf;

public class ClusterServiceTest {

	@Test
	public void deveriaRetornarOsDataNodesConectadosAoZookeeper() throws Exception {
		//Dado
		NodeStatus dataNode1 = new NodeStatus("host1", "127.0.0.1", 16000, 16001, 17000, 17001, 18000, 18001, 15001, 15001);
		
		ZooKeeper zk =  mock(ZooKeeper.class);
		when(zk.getChildren(ZkConf.DATANODES_PATH, false)).thenReturn(Arrays.asList("host1"));
		when(zk.getData(ZkConf.DATANODES_PATH + "/"+ "host1", false, null)).thenReturn(Serializer.fromObject(dataNode1));
		
		//Quando
		List<NodeStatus> dataNodes = ListsManager.getDataNodesConnected();
		
		//Então
		assertThat(dataNodes, contains(dataNode1));
	}
	
}
