package br.edu.ifpi.jazida.client;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import br.edu.ifpi.jazida.node.NodeStatus;

public class RoundRobinPartitionPolicyTest {

	@Test
	public void deveriaRetornarOsNosCircularmente() {
		//Dado
		List<NodeStatus> nodes = new ArrayList<NodeStatus>();
		NodeStatus node1 = new NodeStatus("host1", "127.0.0.1",18001, 17001, 16001, 15001, 14001, 13001, 12001, 12001);
		NodeStatus node2 = new NodeStatus("host2", "127.0.0.1",18002, 17002, 16002, 15002, 14002, 13002, 12002, 12002);
		NodeStatus node3 = new NodeStatus("host3", "127.0.0.1",18003, 17003, 16003, 15003, 14003, 13003, 12003, 12003);
		
		nodes.add(node1);
		nodes.add(node2);
		nodes.add(node3);

		//Quando
		RoundRobinPartitionPolicy policy = new RoundRobinPartitionPolicy();
		policy.addNodes(nodes.toArray(new NodeStatus[nodes.size()]));
		
		//Então
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node2));
		assertTrue(policy.nextNode().equals(node3));
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node2));
		assertTrue(policy.nextNode().equals(node3));
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node2));
		assertTrue(policy.nextNode().equals(node3));
	}
	
	@Test
	public void deveriaRetornarSempreOMesmoNo() {
		//Dado
		List<NodeStatus> nodes = new ArrayList<NodeStatus>();
		NodeStatus node1 = new NodeStatus("host1", "127.0.0.1",18000, 17000, 16000,15000, 14000, 13000, 12000, 12000);
		nodes.add(node1);
		
		//Quando
		RoundRobinPartitionPolicy policy = new RoundRobinPartitionPolicy();
		policy.addNodes(nodes.toArray(new NodeStatus[nodes.size()]));
		
		//Então
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node1));
		assertTrue(policy.nextNode().equals(node1));
	}
	
}

