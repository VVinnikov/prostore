package io.arenadata.dtm.query.execution.plugin.adb.synchronize.service;

import lombok.Data;

@Data
public final class PrepareRequestOfChangesResult {
    private final String newRecordsQuery;
    private final String deletedRecordsQuery;
}
