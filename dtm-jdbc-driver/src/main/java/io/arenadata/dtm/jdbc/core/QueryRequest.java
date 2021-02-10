package io.arenadata.dtm.jdbc.core;


import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.UUID;

/**
 * Sql query request
 */
@Data
@RequiredArgsConstructor
@NoArgsConstructor
public class QueryRequest {
    /**
     * Request UUID
     */
    private UUID requestId;
    /**
     * Datamart
     */
    @NonNull
    private String datamartMnemonic;
    /**
     * sql query
     */
    @NonNull
    private String sql;
    /**
     * query parameters
     */
    private QueryParameters parameters;

    public QueryRequest(UUID requestId,
                        @NonNull String datamartMnemonic,
                        @NonNull String sql,
                        QueryParameters parameters) {
        this.requestId = requestId;
        this.datamartMnemonic = datamartMnemonic;
        this.sql = sql;
        this.parameters = parameters;
    }
}
