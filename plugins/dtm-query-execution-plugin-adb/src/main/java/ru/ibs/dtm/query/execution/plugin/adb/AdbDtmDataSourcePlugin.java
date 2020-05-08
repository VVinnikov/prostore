package ru.ibs.dtm.query.execution.plugin.adb;

import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.DdlService;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

public class AdbDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

  public AdbDtmDataSourcePlugin(
    DdlService ddlService,
    LlrService adbLlrService,
    MpprKafkaService adbMpprKafkaService,
    QueryCostService adbQueryCostService) {
    super(ddlService, adbLlrService, adbMpprKafkaService, adbQueryCostService);
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.ADB;
  }

}
