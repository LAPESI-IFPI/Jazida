package br.edu.ifpi.jazida.writable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import br.edu.ifpi.jazida.writable.WritableUtils;

public class WritableUtilsTest {

	@Test
	public void deveriaConverterUmMapWritableTextParaUmMapString() {
		// Dado
		MapWritable mapWritable = new MapWritable();
		mapWritable.put(new Text("chave"), new Text("valor"));
		
		// Quando
		Map<String, String> map = WritableUtils.convertMapWritableToMap(mapWritable);
		
		// Então
		final String valor = (String) map.get("chave");
		assertThat(valor, is(equalTo("valor")));
	}
	
	@Test
	public void deveriaConverterUmMapStringParaMapWritable() {
		// Dado
		Map<String, String> map = new HashMap<String, String>();
		map.put("chave", "valor");
		
		// Quando
		MapWritable mapWritable = WritableUtils.convertMapToMapWritable(map);
		
		// Então
		final Text valor = (Text) mapWritable.get(new Text("chave"));
		assertThat(valor, is(equalTo(new Text("valor"))));
	}

	@Test
	public void deveriaConverterUmArrayDeTextParaListaDeStrings() {
		// dado
		Text[] textArray = {new Text("texto1"), new Text("texto2"), new Text("texto3")};
		
		// quando
		List<String> novaLista = WritableUtils.convertTextArrayToStringList(textArray);
		
		// então
		assertThat(novaLista, contains("texto1","texto2","texto3"));
	}


	@Test
	public void deveriaConverterUmaListaDeStringsParaArrayDeTexts() {
		// dado
		List<String> lista = new ArrayList<String>();
		lista.add("texto1");
		lista.add("texto2");
		lista.add("texto3");
		
		// quando
		Text[] textArray = WritableUtils.convertStringListToTextArray(lista);
		
		// então
		assertThat(Arrays.asList(textArray), contains(new Text("texto1"), new Text("texto2"), new Text("texto3")));
	}
	
	@Test
	public void deveriaConverterUmIntParaUmIntWritable() {
		// dado
		int numero = 10;

		// quando
		IntWritable intWritable = WritableUtils.convertIntToIntWritable(numero);
		
		// então
		assertThat(intWritable, is(equalTo(new IntWritable(10))));
	}
	
	@Test
	public void deveriaConverterUmaStringParaUmText() {
		// dado
		String texto = "test";

		// quando
		Text text = WritableUtils.convertStringToText(texto);
		
		// então
		assertThat(text, is(equalTo(new Text("test"))));
	}

	@Test
	public void deveriamRetornarNullQuandoRecebemUmParametroNull() {
		// quando
		Text[] textArray = WritableUtils.convertStringListToTextArray(null);
		List<String> stringList = WritableUtils.convertTextArrayToStringList(null);
		Map<String, String> stringMap = WritableUtils.convertMapWritableToMap(null);
		MapWritable mapWritable = WritableUtils.convertMapToMapWritable(null);
		IntWritable intWritable = WritableUtils.convertIntToIntWritable(null);
		Text text = WritableUtils.convertStringToText(null);
		// então
		assertThat(textArray, is(equalTo(null)));
		assertThat(stringList, is(equalTo(null)));
		assertThat(stringMap, is(equalTo(null)));
		assertThat(mapWritable, is(equalTo(null)));
		assertThat(intWritable, is(equalTo(null)));
		assertThat(text, is(equalTo(null)));
	}	
}
