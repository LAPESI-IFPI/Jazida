package br.edu.ifpi.jazida.node;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.Test;

public class NodeStatusTest {

	@Test
	public void deveriaSerIgualAoOutroComTodasPropriedadesIguais() {
		//dado
		NodeStatus host1 = new NodeStatus("host1", "1.1.1.1", 1, 2, 3, 4, 5, 6 ,7,8);
		NodeStatus host2 = new NodeStatus("host1", "1.1.1.1", 1, 2, 3, 4, 5, 6 ,7,8);
		
		//quando
		Boolean resultado = host1.equals(host2);
		
		//então
		assertThat(resultado, is(new Boolean(true)));
	}
	
	@Test
	public void deveriaSerDiferenteComHostnamesDiferentes() {
		//dado
		NodeStatus host1 = new NodeStatus("host1", "1.1.1.1", 1, 2, 3, 4, 5, 6 ,7,8);
		NodeStatus host2 = new NodeStatus("host2", "1.1.1.1", 1, 2, 3, 4, 5, 6 ,7,8);
		//quando
		boolean resultado = host1.equals(host2);
		
		//então
		assertThat(resultado, is(false));
	}
	
	@Test
	public void deveriaSerDiferenteComEnderecoIpDiferentes() {
		//dado
		NodeStatus host1 = new NodeStatus("host1", "1.1.1.1", 1, 2, 3, 4, 5, 6 ,7,8);
		NodeStatus host2 = new NodeStatus("host1", "2.2.2.2", 1, 2, 3, 4, 5, 6 ,7,8);
		
		//quando
		Boolean resultado = host1.equals(host2);
		
		//então
		assertThat(resultado, is(new Boolean(false)));
	}

}
