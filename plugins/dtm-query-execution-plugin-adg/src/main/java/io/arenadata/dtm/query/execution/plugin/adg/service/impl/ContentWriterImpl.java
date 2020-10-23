package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.config.ConsumerConfig;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.config.TopicsConfig;
import io.arenadata.dtm.query.execution.plugin.adg.service.ContentWriter;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;

@Service
public class ContentWriterImpl implements ContentWriter {

  private ObjectMapper yamlMapper;

  @Autowired
  public ContentWriterImpl(@Qualifier("yamlMapper") ObjectMapper yamlMapper) {
    this.yamlMapper = yamlMapper;
  }

  @SneakyThrows
  @Override
  public String toContent(Object config) {
    return yamlMapper.writeValueAsString(config);
  }

  @SneakyThrows
  @Override
  public ConsumerConfig toConsumerConfig(String content) {
    return yamlMapper.readValue(content, new TypeReference<ConsumerConfig>() {});
  }

  @SneakyThrows
  @Override
  public LinkedHashMap<String, TopicsConfig> toTopicsConfig(String content) {
    return yamlMapper.readValue(content, new TypeReference<LinkedHashMap<String, TopicsConfig>>() {
    });
  }
}
