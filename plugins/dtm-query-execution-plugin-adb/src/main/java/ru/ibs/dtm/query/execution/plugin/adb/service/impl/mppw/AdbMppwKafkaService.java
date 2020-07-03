package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MpprKafkaConnectorRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.MpprKafkaConnectorService;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.MppwKafkaService;

@Service("adbMppwKafkaService")
public class AdbMppwKafkaService implements MppwKafkaService<QueryResult> {

	private static final Logger LOG = LoggerFactory.getLogger(AdbMppwKafkaService.class);

	@Autowired
	public AdbMppwKafkaService() {
	}

	@Override
	public void execute(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncHandler) {
		MppwRequest request = context.getRequest();
		//TODO реализовать
	}
}
