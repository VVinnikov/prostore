package io.arenadata.dtm.common.reader;

import io.arenadata.dtm.common.delta.DeltaInformation;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Query request for receiving data
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryRequest {

    /**
     * Request uuid
     */
    private UUID requestId;

    /**
     * Datamart name
     */
    private String datamartMnemonic;

    /**
     * Sql query expression
     */
    private String sql;

    /**
     * Name of environment
     */
    private String envName;

    /**
     * Parameters (optional)
     */
    private List<String> parameters;

    /**
     * Delta Information list
     */
    private List<DeltaInformation> deltaInformations;

    /**
     * Data source type
     */
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
        if (parameters != null) {
            newQueryRequest.setParameters(new ArrayList<>(parameters));
        }
        if (deltaInformations != null) {
            newQueryRequest.setDeltaInformations(new ArrayList<>(deltaInformations));
        }
        newQueryRequest.setEnvName(envName);
        newQueryRequest.setSourceType(sourceType);
        return newQueryRequest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryRequest)) return false;
        QueryRequest that = (QueryRequest) o;
        return requestId.equals(that.requestId) &&
                datamartMnemonic.equals(that.datamartMnemonic) &&
                sql.equals(that.sql) &&
                Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, datamartMnemonic, sql, parameters);
    }
}