package br.edu.ifpi.jazida.writable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WritableUtils {

	/**
	 * Conversão de {@link MapWritable}&lt;Text, Text&gt; para um {@link Map}&lt;String,String&gt;.
	 * 
	 * @param map  Um mapa de MapWritable&lt;Text, Text&gt;.
	 * @return Um mapa de Map&lt;String,String&gt equivalente ao recebido.
	 */
	public static Map<String, String> convertMapWritableToMap(MapWritable map ) {
		if(map == null) {
			return null;
		}
		
		Map<String, String> novoMapa = new HashMap<String, String>();
		Set<Entry<Writable, Writable>> keys = map .entrySet();
		for (Entry<Writable, Writable> each : keys) {
			novoMapa.put(each.getKey().toString(), each.getValue().toString());
		}
		return novoMapa;
	}
	
	/**
	 * Conversão de {@link Map}&lt;Text, Text&gt; para um {@link MapWritable}
	 * 
	 * @param map  Um mapa de MapWritable&lt;String, String&gt;.
	 * @return Um mapa de Map&lt;Text,Text&gt equivalente ao recebido.
	 */
	public static MapWritable convertMapToMapWritable(Map<String, String> map) {
		if(map == null) {
			return null;
		}
		
		MapWritable novoMapa = new MapWritable();
		Set<Entry<String,String>> keys = map.entrySet();
		for (Entry<String, String> each : keys) {
			novoMapa.put(new Text(each.getKey().toString()), new Text(each.getValue().toString()));
		}
		return novoMapa;
	}
	
	/**
	 * Converte uma Lista de {@link Text} para uma lista de {@link String}.
	 * 
	 * @param textArray Um lista do tipo {@link Text}s.
	 * @return Uma lista de {@link String}s equivalente à recebida.
	 */
	public static List<String> convertTextArrayToStringList(Text[] textArray) {
		if(textArray == null) {
			return null;
		}
		List<String> stringList = new ArrayList<String>();
		for (Text text : textArray) {
			stringList.add(text.toString());
		}
		return stringList;
	}



	/**
	 * Converte uma Lista de {@link String} para uma lista de {@link Text}.
	 * 
	 * @param list Uma lista de {@link String}s.
	 * @return Uma lista de {@link Text}s equivalente à recebida.
	 */
	public static Text[] convertStringListToTextArray(List<String> list) {
		if(list == null) {
			return null;
		}
		
		Text[] textArray = new Text[list.size()];
		for (int i=0;i<list.size();i++) {
			textArray[i] = new Text(list.get(i));
		}
		return textArray ;
	}

	public static IntWritable convertIntToIntWritable(Integer numero) {
		if(numero==null) {
			return null;
		}else {
			return new IntWritable(numero);
		}
	}

	public static Text convertStringToText(String texto) {
		if(texto == null) {
			return null;
		}else {
			return new Text(texto);
		}
	}

}
