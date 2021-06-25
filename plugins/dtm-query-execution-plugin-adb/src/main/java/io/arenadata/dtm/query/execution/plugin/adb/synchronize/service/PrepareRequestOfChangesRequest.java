package io.arenadata.dtm.query.execution.plugin.adb.synchronize.service;

import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

@Data
public class PrepareRequestOfChangesRequest {
    private final List<Datamart> datamarts;
    private final String envName;
    private final long deltaNumToBe;
    private final SqlNode viewQuery;
}
