package ru.ibs.dtm.query.execution.core.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlAction;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlQuery;
import ru.ibs.dtm.query.execution.core.service.eddl.EddlExecutor;
import ru.ibs.dtm.query.execution.core.service.eddl.EddlQueryParamExtractor;
import ru.ibs.dtm.query.execution.plugin.api.eddl.EddlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.EddlService;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("coreEddlService")
public class EddlServiceImpl implements EddlService<QueryResult> {

	private static final Logger LOGGER = LoggerFactory.getLogger(EddlServiceImpl.class);

	private final EddlQueryParamExtractor paramExtractor;
	private final Map<EddlAction, EddlExecutor> executors;

	@Autowired
	public EddlServiceImpl(EddlQueryParamExtractor paramExtractor,
						   List<EddlExecutor> eddlExecutors) {
		this.paramExtractor = paramExtractor;
		this.executors = eddlExecutors.stream()
				.collect(Collectors.toMap(EddlExecutor::getAction, it -> it));
	}

	@Override
	public void execute(EddlRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
		paramExtractor.extract(context.getRequest().getQueryRequest(), extractHandler -> {
			if (extractHandler.succeeded()) {
				EddlQuery eddlQuery = extractHandler.result();
				executors.get(eddlQuery.getAction()).execute(eddlQuery, execHandler -> {
					if (execHandler.succeeded()) {
						asyncResultHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
					} else {
						LOGGER.error(execHandler.cause().getMessage());
						asyncResultHandler.handle(Future.failedFuture(execHandler.cause()));
					}
				});
			} else {
				LOGGER.error(extractHandler.cause().getMessage());
				asyncResultHandler.handle(Future.failedFuture(extractHandler.cause()));
			}
		});
	}
}
