package ru.ibs.dtm.jdbc.core;


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
     * List of parameters
     */
    private List<String> parameters;
}
