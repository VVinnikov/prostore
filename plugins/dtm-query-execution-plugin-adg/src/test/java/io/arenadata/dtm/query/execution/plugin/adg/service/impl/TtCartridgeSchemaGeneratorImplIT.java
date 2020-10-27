package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtCartridgeSchemaGenerator;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@ExtendWith(VertxExtension.class)
class TtCartridgeSchemaGeneratorImplIT {

  @Autowired
  private TtCartridgeSchemaGenerator generator;

  @Autowired
  private AdgCartridgeClient client;

  @Autowired
  @Qualifier("yamlMapper")
  private ObjectMapper yamlMapper;

  private Entity entity = new Entity("test.test_", Arrays.asList(
    new EntityField(0,"id", ColumnType.INT.name(), false, 1, 1, null),
    new EntityField(1, "test", ColumnType.VARCHAR.name(), true, null, null, null)
  ));

  @Test
  @SneakyThrows
  void generate(VertxTestContext testContext) throws Throwable {
    client.getSchema(ar1 -> {
      if (ar1.succeeded()) {
        OperationYaml yaml = parseYaml(ar1.result().getData().getCluster().getSchema().getYaml());
        DdlRequestContext context = new DdlRequestContext(new DdlRequest(null, entity));
        generator.generate(context, yaml, ar2 -> {
          if (ar2.succeeded()) {
            testContext.completeNow();
          } else {
            testContext.failNow(ar2.cause());
          }
        });
      } else {
        testContext.failNow(ar1.cause());
      }
    });
    testContext.awaitCompletion(5, TimeUnit.SECONDS);
  }

  private OperationYaml parseYaml(String yaml) {
    OperationYaml operationYaml = null;
    try {
      operationYaml = yamlMapper.readValue(yaml,
        OperationYaml.class);
    } catch (JsonProcessingException e) {
      fail(e);
    }
    return operationYaml;
  }
}