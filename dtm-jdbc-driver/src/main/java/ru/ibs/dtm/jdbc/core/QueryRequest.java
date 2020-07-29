package ru.ibs.dtm.jdbc.core;


import java.util.List;
import java.util.UUID;

/**
 * Запрос на выполнение sql команды
 */
public class QueryRequest {
    /**
     * Идентификатор запроса
     */
    private UUID requestId;
    /**
     * Схема, на которой нужно выполнить команду
     */
    private String datamartMnemonic;
    /**
     * Тело sql-команды
     */
    private String sql;
    /**
     * Необходимые параметры
     */
    private List<String> parameters;

    public QueryRequest() {
    }

    public QueryRequest(UUID requestId, String datamartMnemonic, String sql) {
        this.requestId = requestId;
        this.datamartMnemonic = datamartMnemonic;
        this.sql = sql;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public void setRequestId(UUID requestId) {
        this.requestId = requestId;
    }

    public String getDatamartMnemonic() {
        return datamartMnemonic;
    }

    public void setDatamartMnemonic(String datamartMnemonic) {
        this.datamartMnemonic = datamartMnemonic;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<String> getParameters() {
        return parameters;
    }

    public void setParameters(List<String> parameters) {
        this.parameters = parameters;
    }

    @Override
    public String toString() {
        return "QueryRequest{" +
                "requestId='" + requestId + '\'' +
                ", datamartMnemonic='" + datamartMnemonic + '\'' +
                ", sql='" + sql + '\'' +
                ", parameters=" + parameters +
                '}';
    }
}
