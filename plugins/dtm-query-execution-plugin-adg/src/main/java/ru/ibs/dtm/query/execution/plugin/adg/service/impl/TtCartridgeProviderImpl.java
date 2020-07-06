package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.ResConfig;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.ResOperation;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeClient;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeProvider;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeSchemaGenerator;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class TtCartridgeProviderImpl implements TtCartridgeProvider {

	private TtCartridgeClient client;
	private TtCartridgeSchemaGenerator generator;
	private ObjectMapper yamlMapper;

	@Autowired
	public TtCartridgeProviderImpl(TtCartridgeClient client, TtCartridgeSchemaGenerator generator, @Qualifier("yamlMapper") ObjectMapper yamlMapper) {
		this.client = client;
		this.generator = generator;
		this.yamlMapper = yamlMapper;
	}

	@Override
	public void apply(final DdlRequestContext context, final Handler<AsyncResult<Void>> handler) {
		applySchema(context, ar1 -> {
			if (ar1.succeeded()) {
				setConfig(context, ar2 -> {
					if (ar2.succeeded()) {
						handler.handle(Future.succeededFuture());
					} else {
						handler.handle(Future.failedFuture(ar2.cause()));
					}
				});
			} else {
				handler.handle(Future.failedFuture(ar1.cause()));
			}
		});
	}

	@Override
	public void delete(ClassTable classTable, Handler<AsyncResult<Void>> handler) {
		client.getFiles(ar1 -> {
			if (ar1.succeeded()) {
				val files = ar1.result().getData().getCluster().getConfig()
						.stream().map(ResConfig::toOperationFile).collect(Collectors.toList());
				generator.deleteConfig(classTable, files, ar2 -> {
					if (ar2.succeeded()) {
						client.setFiles(ar2.result(), ar3 -> {
							if (ar3.succeeded()) {
								handler.handle(Future.succeededFuture());
							} else {
								handler.handle(Future.failedFuture(ar3.cause()));
							}
						});
					} else {
						handler.handle(Future.failedFuture(ar2.cause()));
					}
				});
			} else {
				handler.handle(Future.failedFuture(ar1.cause()));
			}
		});
	}

	@SneakyThrows
	public void applySchema(final DdlRequestContext context, final Handler<AsyncResult<Void>> handler) {
		Future.future((Promise<ResOperation> promise) -> client.getSchema(promise))
				.compose(f -> Future.future((Promise<OperationYaml> promise) ->
				{
					try {
						val yaml = yamlMapper.readValue(f.getData().getCluster().getSchema().getYaml(), OperationYaml.class);
						generator.generate(context, yaml, promise);
					} catch (Exception ex) {
						promise.fail(ex);
					}
				})
				.compose(yaml -> Future.future((Promise<String> promise) -> {
					try {
						val yamlResult = yamlMapper.writeValueAsString(yaml);
						if (!yamlResult.isEmpty()) {
							promise.complete(yamlResult);
						} else {
							promise.fail("Empty generated yaml config");
						}
					} catch (Exception ex) {
						promise.fail(ex);
					}
				}))
				.compose(ys -> Future.future((Promise<ResOperation> promise) -> client.setSchema(ys, promise)))
				.onSuccess(success -> handler.handle(Future.succeededFuture()))
				.onFailure(failure -> handler.handle(Future.failedFuture(failure))));
	}

	private void setConfig(final DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
		Future.future((Promise<ResOperation> promise) -> client.getFiles(promise))
				.compose(f -> Future.future((Promise<List<OperationFile>> promise) ->
					{
						val files = f.getData().getCluster().getConfig()
								.stream().map(ResConfig::toOperationFile).collect(Collectors.toList());
						generator.setConfig(context.getRequest().getClassTable(), files, promise);
					})
				.compose(list -> Future.future((Promise<ResOperation> promise) -> client.setFiles(list, promise)))
				.onSuccess(success -> handler.handle(Future.succeededFuture()))
				.onFailure(failure -> handler.handle(Future.failedFuture(failure))));
	}
}
