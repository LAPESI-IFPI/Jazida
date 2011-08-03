package br.edu.ifpi.jazida.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import br.edu.ifpi.opala.utils.Configuration;
import br.edu.ifpi.opala.utils.Property;

public class ConfigurationJazida{
	private static Properties prop = new Properties();
	private static ConfigurationJazida conf;

	private ConfigurationJazida(String confFile) {
		try {
			prop.load(new FileReader(new File(confFile)));
		} catch (FileNotFoundException e) {
			try {
				System.out.println("O arquivo opala.conf não foi encontrado. "
								+ "Crie um arquivo de configuração opala.conf no diretório: "
								+ new File("opala.conf").getCanonicalPath());
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Retorna um instância de {@link Configuration}
	 * 
	 * @return instância de {@link Configuration}
	 */
	public static ConfigurationJazida getInstance() {
		if (conf == null)
			conf = new ConfigurationJazida("opala.conf");
		return conf;
	}

	/**
	 * Seta o arquivo de configuração especificado
	 * 
	 * @param confFile
	 *            caminho e nome do arquivo de configuração
	 */
	public static void setConf(String confFile) {
		conf = new ConfigurationJazida(confFile);
	}

	/**
	 * Retorna o valor da {@link Property} informada como parâmetro
	 * 
	 * @param property
	 *            a propriedade que se deseja obter o valor
	 * @return valor da propriedade informada
	 */
	public String getPropertyValue(PropertyJazida property) {
		return prop.getProperty(property.getPropertyName()).trim();
	}

	
}
