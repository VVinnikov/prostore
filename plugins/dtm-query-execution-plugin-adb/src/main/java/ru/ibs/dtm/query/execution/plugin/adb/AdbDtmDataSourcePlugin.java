package ru.ibs.dtm.query.execution.plugin.adb;

import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.AbstractDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

public class AdbDtmDataSourcePlugin extends AbstractDtmDataSourcePlugin {

	public AdbDtmDataSourcePlugin(
			DdlService<Void> ddlService,
			LlrService<QueryResult> adbLlrService,
			MpprKafkaService<QueryResult> adbMpprKafkaService,
			QueryCostService<Integer> adbQueryCostService) {
		super(ddlService, adbLlrService, adbMpprKafkaService, adbQueryCostService);
	}

	@Override
	public SourceType getSourceType() {
		return SourceType.ADB;
	}

}
