package ru.ibs.dtm.query.execution.core.service.impl;


import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.core.service.SchemaStorageProvider;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class SchemaStorageProviderImpl implements SchemaStorageProvider {

  private String metadataPath;

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaStorageProviderImpl.class);

  private Map<String, JsonObject> listOfStorage = new HashMap<>();

  public SchemaStorageProviderImpl(@Value("${server.metadata.path}") String path) {
    this.metadataPath = path;
  }

  @Override
  public void getLogicalSchema(Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO: временно пока нет сервиса
    asyncResultHandler.handle(Future.succeededFuture(listOfStorage.get("test_datamart")));
  }

  @PostConstruct
  public void init() {
    String text = readTextFromFile();
    List<Map<String, Object>> list = new JsonArray(text).getList();
    list.forEach(it -> {
      JsonObject obj = new JsonObject(it);
      listOfStorage.put(obj.getString("mnemonic"), obj);
    });
  }

  private String readTextFromFile() {
    File file = null;
    String result = null;
    try {
      String fileAbsolutePath = getFileAbsolutePath(metadataPath);
      log.info("Путь до файла метаданных" + fileAbsolutePath + File.separator + "meta_data.json");
      file = new File(fileAbsolutePath + File.separator + "meta_data.json");
      result = new String(Files.readAllBytes(file.toPath()));
    } catch (IOException e) {
      LOGGER.error("Ошибка получения схемы", e);
    }
    return result;
  }

  private String getFileAbsolutePath(String fileSchemasDir) throws IOException {
    if (org.springframework.util.StringUtils.isEmpty(fileSchemasDir)) {
      throw new RuntimeException("Не задана директория файлов метаданных!");
    }
    return new File(fileSchemasDir).getAbsolutePath();
  }
}
