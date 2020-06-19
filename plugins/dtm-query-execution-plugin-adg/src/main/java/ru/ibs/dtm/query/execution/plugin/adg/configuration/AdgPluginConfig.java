package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adg.AdgDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.DdlService;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

@Configuration
public class AdgPluginConfig {

  @Bean("adgDtmDataSourcePlugin")
  public AdgDataSourcePlugin adbDataSourcePlugin(
    @Qualifier("adgDdlService") DdlService<Void> ddlService,
    @Qualifier("adgLlrService") LlrService<QueryResult> llrService,
    @Qualifier("adgMpprKafkaService") MpprKafkaService<QueryResult> mpprKafkaService,
    @Qualifier("adgQueryCostService") QueryCostService<Integer> queryCostService) {
    return new AdgDataSourcePlugin(ddlService, llrService, mpprKafkaService, queryCostService);
  }
}
