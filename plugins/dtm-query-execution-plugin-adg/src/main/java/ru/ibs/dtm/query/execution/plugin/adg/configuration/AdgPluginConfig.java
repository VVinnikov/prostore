package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.plugin.adg.AdgDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.DdlService;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

@Configuration
public class AdgPluginConfig {

  @Bean("adgDtmDataSourcePlugin")
  public AdgDataSourcePlugin adbDataSourcePlugin(
    @Qualifier("adgDdlService") DdlService ddlService,
    @Qualifier("adgLlrService") LlrService llrService,
    @Qualifier("adgMpprKafkaService") MpprKafkaService mpprKafkaService,
    @Qualifier("adgQueryCostService") QueryCostService queryCostService) {
    return new AdgDataSourcePlugin(ddlService, llrService, mpprKafkaService, queryCostService);
  }
}
