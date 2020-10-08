package ru.ibs.dtm.common.reader;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.ibs.dtm.common.delta.DeltaInformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Запрос на извлечение данных
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
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

    private String systemName;

    /**
     * Параметры(необязательно)
     */
    private List<String> parameters;

    /**
     *  Delta Information
     */
    private List<DeltaInformation> deltaInformations;
    private SourceType sourceType;

    public QueryRequest(UUID requestId, String datamartMnemonic, String sql) {
        this.requestId = requestId;
        this.datamartMnemonic = datamartMnemonic;
        this.sql = sql;
    }

    public QueryRequest copy() {
        QueryRequest newQueryRequest = new QueryRequest();
        newQueryRequest.setSql(sql);
        newQueryRequest.setDatamartMnemonic(datamartMnemonic);
        newQueryRequest.setRequestId(requestId);
        newQueryRequest.setSubRequestId(subRequestId);
        if (parameters != null) {
            newQueryRequest.setParameters(new ArrayList<>(parameters));
        }
        if (deltaInformations != null) {
            newQueryRequest.setDeltaInformations(new ArrayList<>(deltaInformations));
        }
        newQueryRequest.setSystemName(systemName);
        newQueryRequest.setSourceType(sourceType);
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
}
