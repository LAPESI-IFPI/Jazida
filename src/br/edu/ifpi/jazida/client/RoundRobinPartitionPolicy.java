package br.edu.ifpi.jazida.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import br.edu.ifpi.jazida.node.NodeStatus;

/**
 * Implementa uma estratégia simples de distribuição de forma circular.
 * 
 * @author Aécio Santos
 * 
 */
public class RoundRobinPartitionPolicy implements PartitionPolicy<NodeStatus> {

	private List<NodeStatus> nodes = new ArrayList<NodeStatus>();
	private int currentNode = 0;

	@Override
	public synchronized NodeStatus nextNode() {
		
		if (currentNode >= nodes.size()) {
			currentNode = 0;
		}
		
		NodeStatus node = nodes.get(currentNode);
		currentNode++;
		
		return node;
	}

	@Override
	public void addNodes(NodeStatus... liveNodes) {
		nodes.addAll(Arrays.asList(liveNodes));
	}
	
	@Override
	public void removeAllNodes(NodeStatus... deadNodes) {
		nodes.removeAll(Arrays.asList(deadNodes));
	}
	
	@Override
	public void clear(){
		if(!nodes.isEmpty())
			nodes.clear();
	}

	@Override
	public synchronized void removeNode(NodeStatus node) {
		if (nodes.remove(node))
			nodes.remove(nodes.indexOf(node));
	}

}
