package io.arenadata.dtm.query.execution.core.dto.dml;

import lombok.Data;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSnapshot;

import java.util.function.Consumer;

@Data
public class SnapshotReplaceAction {
    private final Consumer<SqlNode> consumer;
    private final SqlIdentifier from;
    private String viewName;
    private SqlSnapshot to;

    public SnapshotReplaceAction(SqlIdentifier from, Consumer<SqlNode> consumer) {
        this.consumer = consumer;
        this.from = from;
    }

    public void run() {
        consumer.accept(to);
    }
}
