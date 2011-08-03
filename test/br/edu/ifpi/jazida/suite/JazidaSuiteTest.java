package br.edu.ifpi.jazida.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import br.edu.ifpi.jazida.client.ImageIndexerClientTest;
import br.edu.ifpi.jazida.client.ImageSearcherClientTest;
import br.edu.ifpi.jazida.client.RoundRobinPartitionPolicyTest;
import br.edu.ifpi.jazida.client.TextIndexerClientTest;
import br.edu.ifpi.jazida.client.TextSearcherClientTest;
import br.edu.ifpi.jazida.node.ClusterServiceTest;
import br.edu.ifpi.jazida.node.NodeStatusTest;
import br.edu.ifpi.jazida.util.JazidaConfTest;
import br.edu.ifpi.jazida.writable.WritableUtilsTest;


@RunWith(Suite.class)
@Suite.SuiteClasses({
	RoundRobinPartitionPolicyTest.class,
	NodeStatusTest.class,
	ClusterServiceTest.class,
	JazidaConfTest.class,
	WritableUtilsTest.class,
	TextIndexerClientTest.class,
	TextSearcherClientTest.class,
	ImageIndexerClientTest.class,
	ImageSearcherClientTest.class
})

/**
 * Esta classe executa todos os métodos de testes contidos nas classes de teste 
 * definida acima devendo ser executado como "JUnit Test"
 *  
 * @author Aécio Santos
 *
 */
public class JazidaSuiteTest {
	
}