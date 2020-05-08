package ru.ibs.dtm.query.execution.plugin.adg;

import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.DdlService;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

public class AdgDataSourcePlugin extends AbstractDtmDataSourcePlugin {

  public AdgDataSourcePlugin(DdlService ddlService,
                             LlrService llrService,
                             MpprKafkaService mpprKafkaService,
                             QueryCostService queryCostService) {
    super(ddlService, llrService, mpprKafkaService, queryCostService);
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.ADG;
  }

}
