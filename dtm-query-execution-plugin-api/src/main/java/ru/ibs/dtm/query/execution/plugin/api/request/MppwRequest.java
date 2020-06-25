package ru.ibs.dtm.query.execution.plugin.api.request;

import io.vertx.core.json.JsonObject;
import ru.ibs.dtm.common.plugin.exload.QueryLoadParam;
import ru.ibs.dtm.common.reader.QueryRequest;

/**
 * dto для выполнения MppwKafka
 */
public class MppwRequest extends DatamartRequest {

  /**
   * Парамеры выполнения загрузки
   */
  private QueryLoadParam queryLoadParam;
  /**
   * Логическая схема
   */
  private JsonObject avroSchema;

  /**
   * Хост zookeeper'а
   */
  private String zookeeperHost;
  /**
   * Порт zookeeper'а
   */
  private int zookeeperPort;
  /**
   * Название выгружаеиого топика
   */
  private String topic;

  public MppwRequest(QueryRequest queryRequest, QueryLoadParam queryLoadParam, JsonObject avroSchema) {
    super(queryRequest);
    this.queryLoadParam = queryLoadParam;
    this.avroSchema = avroSchema;
  }

  public QueryLoadParam getQueryLoadParam() {
    return queryLoadParam;
  }

  public void setQueryLoadParam(QueryLoadParam queryLoadParam) {
    this.queryLoadParam = queryLoadParam;
  }

  public JsonObject getAvroSchema() {
    return avroSchema;
  }

  public void setAvroSchema(JsonObject avroSchema) {
    this.avroSchema = avroSchema;
  }

  public String getZookeeperHost() {
    return zookeeperHost;
  }

  public void setZookeeperHost(String zookeeperHost) {
    this.zookeeperHost = zookeeperHost;
  }

  public int getZookeeperPort() {
    return zookeeperPort;
  }

  public void setZookeeperPort(int zookeeperPort) {
    this.zookeeperPort = zookeeperPort;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }
}
