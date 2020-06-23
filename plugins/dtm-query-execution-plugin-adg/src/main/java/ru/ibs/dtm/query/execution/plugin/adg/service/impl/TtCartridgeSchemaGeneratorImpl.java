package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.KafkaProperties;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.config.ConsumerConfig;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.config.TopicsConfig;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.schema.*;
import ru.ibs.dtm.query.execution.plugin.adg.service.ContentWriter;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeSchemaGenerator;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;

@Service
public class TtCartridgeSchemaGeneratorImpl implements TtCartridgeSchemaGenerator {

  private KafkaProperties kafkaProperties;
  private ContentWriter contentWriter;

  @Autowired
  public TtCartridgeSchemaGeneratorImpl(@Qualifier("adgKafkaProperties") KafkaProperties kafkaProperties, ContentWriter contentWriter) {
    this.kafkaProperties = kafkaProperties;
    this.contentWriter = contentWriter;
  }

  @Override
  public void generate(ClassTable classTable, OperationYaml yaml, Handler<AsyncResult<OperationYaml>> handler) {
    if (yaml.getSpaces().isEmpty()) {
      yaml.setSpaces(new LinkedHashMap<>());
    }
    val spaces = yaml.getSpaces();
    int indexComma = classTable.getName().indexOf(".");
    String table = classTable.getName().substring(indexComma + 1).toLowerCase();
    spaces.put(table + ACTUAL_POSTFIX, create(classTable.getFields()));
    spaces.put(table + HISTORY_POSTFIX, create(classTable.getFields()));
    handler.handle(Future.succeededFuture(yaml));
  }

  @Override
  public void setConfig(ClassTable classTable, List<OperationFile> files, Handler<AsyncResult<List<OperationFile>>> handler) {
    setConsumerConfig(files, handler, classTable);
    setTopicConfig(files, handler, classTable);
  }

  @Override
  public void deleteConfig(ClassTable classTable, List<OperationFile> files, Handler<AsyncResult<List<OperationFile>>> handler) {
    //setConsumerConfig
    val consumerConfig = files.stream().filter(it -> it.getFilename().equalsIgnoreCase(ConsumerConfig.FILE_NAME)).findFirst();
    if (consumerConfig.isPresent()) {
      val config = contentWriter.toConsumerConfig(consumerConfig.get().getContent());
      val properties = kafkaProperties.getAdmin();
      val topic = String.format(properties.getAdgUploadRq(), classTable.getName(), classTable.getSchema());
      config.getTopics().remove(topic);
      files.stream().filter(it -> it.getFilename().equalsIgnoreCase(ConsumerConfig.FILE_NAME)).findFirst().ifPresent(it -> it.setContent(contentWriter.toContent(config)));
    } else {
      handler.handle(Future.failedFuture(new Exception("Не найден искомый файл конфигурации: " + ConsumerConfig.FILE_NAME)));
    }

    //setTopicConfig
    val topicsConfig = files.stream().filter(it -> it.getFilename().equalsIgnoreCase(TopicsConfig.FILE_NAME)).findFirst();
    if (topicsConfig.isPresent()) {
      val config = contentWriter.toTopicsConfig(topicsConfig.get().getContent());
      val properties = kafkaProperties.getAdmin();
      val topic = String.format(properties.getAdgUploadRq(), classTable.getName(), classTable.getSchema());
      config.remove(topic, createTopicConfig(properties, classTable));
      files.stream().filter(it -> it.getFilename().equalsIgnoreCase(TopicsConfig.FILE_NAME)).findFirst().ifPresent(it -> it.setContent(contentWriter.toContent(config)));
      handler.handle(Future.succeededFuture(files));
    } else {
      handler.handle(Future.failedFuture(new Exception("Не найден искомый файл конфигурации: " + ConsumerConfig.FILE_NAME)));
    }
  }

  private void setTopicConfig(List<OperationFile> files, Handler<AsyncResult<List<OperationFile>>> handler, ClassTable classTable) {
    val topicsConfig = files.stream().filter(it -> it.getFilename().equalsIgnoreCase(TopicsConfig.FILE_NAME)).findFirst();
    if (topicsConfig.isPresent()) {
      val config = contentWriter.toTopicsConfig(topicsConfig.get().getContent());
      val properties = kafkaProperties.getAdmin();
      val topic = String.format(properties.getAdgUploadRq(), classTable.getName(), classTable.getSchema());
      config.put(topic, createTopicConfig(properties, classTable));
      files.stream().filter(it -> it.getFilename().equalsIgnoreCase(TopicsConfig.FILE_NAME)).findFirst().ifPresent(it -> it.setContent(contentWriter.toContent(config)));
      handler.handle(Future.succeededFuture(files));
    } else {
      handler.handle(Future.failedFuture(new Exception("Не найден искомый файл конфигурации: " + ConsumerConfig.FILE_NAME)));
    }
  }

  private void setConsumerConfig(List<OperationFile> files, Handler<AsyncResult<List<OperationFile>>> handler, ClassTable classTable) {
    val consumerConfig = files.stream().filter(it -> it.getFilename().equalsIgnoreCase(ConsumerConfig.FILE_NAME)).findFirst();
    if (consumerConfig.isPresent()) {
      val config = contentWriter.toConsumerConfig(consumerConfig.get().getContent());
      val properties = kafkaProperties.getAdmin();
      val topic = String.format(properties.getAdgUploadRq(), classTable.getName(), classTable.getSchema());
      config.getTopics().add(topic);
      files.stream().filter(it -> it.getFilename().equalsIgnoreCase(ConsumerConfig.FILE_NAME)).findFirst().ifPresent(it -> it.setContent(contentWriter.toContent(config)));
    } else {
      handler.handle(Future.failedFuture(new Exception("Не найден искомый файл конфигурации: " + ConsumerConfig.FILE_NAME)));
    }
  }

  public static Space create(List<ClassField> fields) {
    return new Space(
      getAttributes(fields),
      false,
      SpaceEngines.MEMTX,
      false,
      Collections.singletonList(ID),
      Arrays.asList(
        new SpaceIndex(true, Arrays.asList(
          new SpaceIndexPart(ID, SpaceAttributeTypes.NUMBER.getName(), false),
          new SpaceIndexPart(SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER.getName(), false)
        ), SpaceIndexTypes.TREE, ID),
        new SpaceIndex(false, Collections.singletonList(
          new SpaceIndexPart(BUCKET_ID, SpaceAttributeTypes.UNSIGNED.getName(), false)
        ), SpaceIndexTypes.TREE, BUCKET_ID)
      ));
  }
  //Порядок следования снихронизован с AVRO схемой
  private static List<SpaceAttribute> getAttributes(List<ClassField> fields) {
    List<SpaceAttribute> attributes = fields.stream().map(TtCartridgeSchemaGeneratorImpl::toAttribute).collect(Collectors.toList());
    attributes.addAll(
      Arrays.asList(
        new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER),
        new SpaceAttribute(false, SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER),
        new SpaceAttribute(true,  SYS_TO_FIELD, SpaceAttributeTypes.NUMBER),
        new SpaceAttribute(false, BUCKET_ID, SpaceAttributeTypes.UNSIGNED))
    );
    return attributes;
  }

  private static SpaceAttribute toAttribute(ClassField field) {
    return new SpaceAttribute(field.getNull(), field.getName(), SpaceAttributeTypeUtil.toAttributeType(field.getType()));
  }

  private TopicsConfig createTopicConfig(KafkaAdminProperty property, ClassTable classTable) {
    val adgUploadRq = String.format(property.getAdgUploadRq(), classTable.getName(), classTable.getSchema());
    val adgUploadRs = String.format(property.getAdgUploadRs(), classTable.getName(), classTable.getSchema());
    val adgUploadErr = String.format(property.getAdgUploadErr(), classTable.getName(), classTable.getSchema());
    return new TopicsConfig(adgUploadErr, "", classTable.getName().toLowerCase() + ACTUAL_POSTFIX, adgUploadRq.replace(".", "-"), adgUploadRs);
  }
}
