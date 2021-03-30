package io.arenadata.dtm.common.reader;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;
import java.util.UUID;

/**
 * Input execution query request
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InputQueryRequest {

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
     * Parameters (optional)
     */
    private QueryParameters parameters;
    /**
     * Executable query or for caching
     */
    private boolean isExecutable = true;

    public InputQueryRequest copy() {
        InputQueryRequest newQueryRequest = new InputQueryRequest();
        newQueryRequest.setSql(sql);
        newQueryRequest.setDatamartMnemonic(datamartMnemonic);
        newQueryRequest.setRequestId(requestId);
        if (parameters != null) {
            newQueryRequest.setParameters(parameters.copy());
        }
        return newQueryRequest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InputQueryRequest that = (InputQueryRequest) o;
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
