package io.arenadata.dtm.query.execution.plugin.adg.configuration;

import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgClientPool;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.AdgClientFactory;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.AdgResultTranslatorImpl;
import lombok.val;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(TarantoolDatabaseProperties.class)
public class TarantoolAutoConfiguration {

  @Bean("adgTtPool")
  public AdgClientPool ttPool(TarantoolDatabaseProperties tarantoolProperties) {
    val resultTranslator = new AdgResultTranslatorImpl();
    val factory = new AdgClientFactory(tarantoolProperties, resultTranslator);
    val config = new GenericObjectPoolConfig<AdgClient>();
    config.setJmxEnabled(false);
    return new AdgClientPool(factory, config);
  }
}
