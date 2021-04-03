package io.arenadata.dtm.query.execution.core.eddl.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Eddl query
 */
@Data
@AllArgsConstructor
@RequiredArgsConstructor
public abstract class EddlQuery {

    /**
     * Eddl query type
     */
    @NonNull
    private EddlAction action;

    /**
     * Schema name
     */
    private String schemaName;

    /**
     * table name
     */
    private String tableName;

}
