package utils;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JsonUtils {
  public static JsonObject init(String fileName, String mnemonicName) {
    Map<String, JsonObject> listOfStorage = new HashMap<>();
    String text = readTextFromFile(fileName);
    List<Map<String, Object>> list = new JsonArray(text).getList();
    list.forEach(it -> {
      JsonObject obj = new JsonObject(it);
      listOfStorage.put(obj.getString("mnemonic"), obj);
    });
    return listOfStorage.get(mnemonicName);
  }

  private static String readTextFromFile(String fileName) {
    File file = null;
    String result = null;
    try {
      file = new ClassPathResource(fileName).getFile();
      result = new String(Files.readAllBytes(file.toPath()));
    } catch (IOException e) {
      log.error("Ошибка получения схемы", e);
    }
    return result;
  }
}
