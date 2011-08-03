package br.edu.ifpi.jazida.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

public class JazidaConfTest {

	@Test
	public void deveriaRetornarAlgumHostDoArquivoDeConfiguracao() {
		assertThat(ZkConf.ZOOKEEPER_SERVERS, is(instanceOf(String.class)));
	}

}
