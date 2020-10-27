package io.arenadata.dtm.query.execution.plugin.adg;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@ConfigurationPropertiesScan(basePackages = "io.arenadata.dtm.query.execution.plugin.adg.configuration")
public class AdgPluginApplication {
	public static void main(String[] args) {
		SpringApplication.run(AdgPluginApplication.class, args);
	}
}