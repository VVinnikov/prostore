package ru.ibs.dtm.common.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Запрос на извлечение данных
 */
public class QueryRequest {

    /**
     * UUID базового запроса, пришедшего в ПОДД
     */
    private UUID requestId;

    /**
     * UUID подзапроса, выделенного из базового, sql-выражение которого передается Витрине
     */
    private String subRequestId;

    /**
     * мнемоника Витрины, на которой необходимо выполнить запрос
     */
    private String datamartMnemonic;

    /**
     * SQL-запрос
     */
    private String sql;

    /**
     * Параметры(необязательно)
     */
    private List<String> parameters;

    public UUID getRequestId() {
        return requestId;
    }

    public void setRequestId(UUID requestId) {
        this.requestId = requestId;
    }

    public String getSubRequestId() {
        return subRequestId;
    }

    public void setSubRequestId(String subRequestId) {
        this.subRequestId = subRequestId;
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

    public QueryRequest copy() {
        QueryRequest newQueryRequest = new QueryRequest();
        newQueryRequest.setSql(sql);
        newQueryRequest.setDatamartMnemonic(datamartMnemonic);
        newQueryRequest.setRequestId(requestId);
        newQueryRequest.setSubRequestId(subRequestId);
        List<String> paramsCopy = null;
        if (parameters != null) {
            paramsCopy = new ArrayList<>();
            paramsCopy.addAll(parameters);
        }
        newQueryRequest.setParameters(paramsCopy);
        return newQueryRequest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryRequest)) return false;
        QueryRequest that = (QueryRequest) o;
        return requestId.equals(that.requestId) &&
                subRequestId.equals(that.subRequestId) &&
                datamartMnemonic.equals(that.datamartMnemonic) &&
                sql.equals(that.sql) &&
                Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, subRequestId, datamartMnemonic, sql, parameters);
    }

    @Override
    public String toString() {
        return "QueryRequest{" +
                "requestId=" + requestId +
                ", subRequestId='" + subRequestId + '\'' +
                ", datamartMnemonic='" + datamartMnemonic + '\'' +
                ", sql='" + sql + '\'' +
                ", parameters=" + parameters +
                '}';
    }
}
