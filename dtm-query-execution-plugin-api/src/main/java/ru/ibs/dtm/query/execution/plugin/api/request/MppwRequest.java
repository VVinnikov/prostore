package ru.ibs.dtm.query.execution.plugin.api.request;

import io.vertx.core.json.JsonObject;
import ru.ibs.dtm.common.plugin.exload.QueryLoadParam;
import ru.ibs.dtm.common.reader.QueryRequest;

/**
 * dto для выполнения MppwKafka
 */
public class MppwRequest extends DatamartRequest {

    /**
     * логический признак начала загрузки
     */
    private Boolean isLoadStart;
    /**
     * авро схема данных
     */
    private JsonObject schema;
    /**
     * Парамеры выполнения загрузки
     */
    private QueryLoadParam queryLoadParam;
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

    public MppwRequest(QueryRequest queryRequest, QueryLoadParam queryLoadParam, JsonObject schema) {
        super(queryRequest);
        this.queryLoadParam = queryLoadParam;
        this.schema = schema;
    }

    public QueryLoadParam getQueryLoadParam() {
        return queryLoadParam;
    }

    public void setQueryLoadParam(QueryLoadParam queryLoadParam) {
        this.queryLoadParam = queryLoadParam;
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

    public Boolean getLoadStart() {
        return isLoadStart;
    }

    public void setLoadStart(Boolean loadStart) {
        isLoadStart = loadStart;
    }

    public JsonObject getSchema() {
        return schema;
    }

    public void setSchema(JsonObject schema) {
        this.schema = schema;
    }
}
