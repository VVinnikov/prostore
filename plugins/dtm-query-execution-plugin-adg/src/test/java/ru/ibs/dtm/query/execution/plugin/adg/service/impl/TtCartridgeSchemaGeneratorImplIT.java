package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ClassTypes;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.ResConfig;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeClient;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeSchemaGenerator;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@ExtendWith(VertxExtension.class)
class TtCartridgeSchemaGeneratorImplIT {

  @Autowired
  private TtCartridgeSchemaGenerator generator;

  @Autowired
  private TtCartridgeClient client;

  @Autowired
  @Qualifier("yamlMapper")
  private ObjectMapper yamlMapper;

  private ClassTable classTable = new ClassTable("test.test_", Arrays.asList(
    new ClassField("id", ClassTypes.INT.name(), false, 1, 1, null),
    new ClassField("test", ClassTypes.VARCHAR.name(), true, null, null, null)
  ));

  @Test
  @SneakyThrows
  void generate(VertxTestContext testContext) throws Throwable {
    client.getSchema(ar1 -> {
      if (ar1.succeeded()) {
        OperationYaml yaml = parseYaml(ar1.result().getData().getCluster().getSchema().getYaml());
        DdlRequestContext context = new DdlRequestContext(new DdlRequest(null, classTable));
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

  @Test
  void setConfig(VertxTestContext testContext) throws Throwable {
    client.getFiles(ar1 -> {
      if (ar1.succeeded()) {
        val files = ar1.result().getData().getCluster().getConfig()
          .stream().map(ResConfig::toOperationFile).collect(Collectors.toList());
        generator.setConfig(classTable, files, ar2 -> {
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
