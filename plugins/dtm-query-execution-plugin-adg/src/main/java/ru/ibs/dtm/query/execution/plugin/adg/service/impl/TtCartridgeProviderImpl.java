package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.ResConfig;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeClient;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeProvider;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeSchemaGenerator;

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
  public void apply(ClassTable classTable, Handler<AsyncResult<Void>> handler) {
    applySchema(classTable, ar1 -> {
      if (ar1.succeeded()) {
        setConfig(classTable, ar2 -> {
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
  public void applySchema(ClassTable classTable, Handler<AsyncResult<Void>> handler) {
    client.getSchema(ar1 -> {
      if (ar1.succeeded()) {
        try {
          val yaml = yamlMapper.readValue(ar1.result().getData().getCluster().getSchema().getYaml(),
            OperationYaml.class);
          generator.generate(classTable, yaml, ar2 -> {
            if (ar2.succeeded()) {
              val yamlResult = convertYaml(ar2.result(), handler);
              if (!yamlResult.isEmpty()) {
                client.setSchema(yamlResult, ar3 -> {
                  if (ar3.succeeded()) {
                    handler.handle(Future.succeededFuture());
                  } else {
                    handler.handle(Future.failedFuture(ar3.cause()));
                  }
                });
              }
            } else {
              handler.handle(Future.failedFuture(ar2.cause()));
            }
          });
        } catch (JsonProcessingException e) {
          handler.handle(Future.failedFuture(e));
        }
      } else {
        handler.handle(Future.failedFuture(ar1.cause()));
      }
    });
  }

  private String convertYaml(OperationYaml objYaml, Handler<AsyncResult<Void>> handler) {
    String yaml = "";
    try {
      yaml = yamlMapper.writeValueAsString(objYaml);
    } catch (JsonProcessingException e) {
      handler.handle(Future.failedFuture(e));
    }
    return yaml;
  }

  private void setConfig(ClassTable classTable, Handler<AsyncResult<Void>> handler) {
    client.getFiles(ar1 -> {
      if (ar1.succeeded()) {
        val files = ar1.result().getData().getCluster().getConfig()
          .stream().map(ResConfig::toOperationFile).collect(Collectors.toList());
        generator.setConfig(classTable, files, ar2 -> {
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
}
