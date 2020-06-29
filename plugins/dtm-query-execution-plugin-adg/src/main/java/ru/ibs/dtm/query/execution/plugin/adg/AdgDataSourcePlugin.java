package ru.ibs.dtm.query.execution.plugin.adg;

import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

public class AdgDataSourcePlugin extends AbstractDtmDataSourcePlugin {

  public AdgDataSourcePlugin(
          DdlService<Void> ddlService,
          LlrService<QueryResult> llrService,
          MpprKafkaService<QueryResult> mpprKafkaService,
          QueryCostService<Integer> adgQueryCostService) {
    super(ddlService, llrService, mpprKafkaService, adgQueryCostService);
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.ADG;
  }

}
