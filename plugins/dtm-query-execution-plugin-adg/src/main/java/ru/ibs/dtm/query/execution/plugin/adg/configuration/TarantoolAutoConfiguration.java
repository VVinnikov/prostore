package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import lombok.val;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtClient;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtPool;
import ru.ibs.dtm.query.execution.plugin.adg.service.impl.TtClientFactory;
import ru.ibs.dtm.query.execution.plugin.adg.service.impl.TtResultTranslatorImpl;

@Configuration
@EnableConfigurationProperties(TarantoolDatabaseProperties.class)
public class TarantoolAutoConfiguration {

  @Bean("adgTtPool")
  public TtPool ttPool(TarantoolDatabaseProperties tarantoolProperties) {
    val resultTranslator = new TtResultTranslatorImpl();
    val factory = new TtClientFactory(tarantoolProperties, resultTranslator);
    val config = new GenericObjectPoolConfig<TtClient>();
    config.setJmxEnabled(false);
    return new TtPool(factory, config);
  }
}
