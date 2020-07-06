package ru.ibs.dtm.query.execution.plugin.api.request;

import io.vertx.core.json.JsonObject;
import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.common.reader.QueryRequest;

/**
 * dto для выполнения MpprKafka
 */
public class MpprRequest extends DatamartRequest {

  /**
   * Парамеры выполнения выгрузки
   */
  private QueryExloadParam queryExloadParam;
  /**
   * схема данных
   */
  private JsonObject schema;

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

  public MpprRequest(QueryRequest queryRequest, QueryExloadParam queryExloadParam, JsonObject schema) {
    super(queryRequest);
    this.queryExloadParam = queryExloadParam;
    this.schema = schema;
  }


  public QueryExloadParam getQueryExloadParam() {
    return queryExloadParam;
  }

  public void setQueryExloadParam(QueryExloadParam queryExloadParam) {
    this.queryExloadParam = queryExloadParam;
  }

  public JsonObject getSchema() {
    return schema;
  }

  public void setSchema(JsonObject schema) {
    this.schema = schema;
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

  @Override
  public String toString() {
    return "MpprKafkaRequest{" +
      "queryExloadParam=" + queryExloadParam +
      ", schema=" + schema +
      '}';
  }
}
