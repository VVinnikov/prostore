package ru.ibs.dtm.query.execution.plugin.adb.configuration;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.AdbDtmDataSourcePlugin;
import ru.ibs.dtm.query.execution.plugin.api.service.DdlService;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

@Configuration
public class AdbDataSourcePluginConfig {

	@Bean("adbDtmDataSourcePlugin")
	public AdbDtmDataSourcePlugin adbDataSourcePlugin(
			@Qualifier("adbDdlService") DdlService<Void> ddlService,
			@Qualifier("adbLlrService") LlrService<QueryResult> llrService,
			@Qualifier("adbMpprKafkaService") MpprKafkaService<QueryResult> mpprKafkaService,
			@Qualifier("adbQueryCostService") QueryCostService<Integer> queryCostService) {
		return new AdbDtmDataSourcePlugin(
				ddlService,
				llrService,
				mpprKafkaService,
				queryCostService);
	}
}
