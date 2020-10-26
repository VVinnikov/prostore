package io.arenadata.dtm.query.execution.core.dto.dml;

import lombok.Data;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSnapshot;

import java.util.function.Consumer;

@Data
public class ViewReplaceAction {
    private final Consumer<SqlNode> consumer;
    private final boolean needWrap;
    private final SqlNode from;
    private DatamartViewPair viewPair;
    private SqlNode to;

    public ViewReplaceAction(SqlNode from, Consumer<SqlNode> consumer) {
        this(from, false, consumer);
    }

    public ViewReplaceAction(SqlNode from, boolean needWrap, Consumer<SqlNode> consumer) {
        this.needWrap = needWrap;
        this.consumer = consumer;
        this.from = from;
        this.viewPair = new DatamartViewPair(getDatamart(from), getViewName(from));
    }

    private String getViewName(SqlNode from) {
        if (from instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) from;
            String name = identifier.names.size() > 1 ? identifier.names.get(1) : identifier.names.get(0);
            return name.toLowerCase();
        } else if (from instanceof SqlSnapshot) {
            return getViewName(((SqlSnapshot) from).getTableRef());
        } else throw new IllegalArgumentException("Node required instance of SqlIdentifier or SqlSnapshot");
    }

    private String getDatamart(SqlNode from) {
        if (from instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) from;
            return identifier.names.size() > 1 ? identifier.names.get(0).toLowerCase() : null;
        } else if (from instanceof SqlSnapshot) {
            return getDatamart(((SqlSnapshot) from).getTableRef());
        } else throw new IllegalArgumentException("Node required instance of SqlIdentifier or SqlSnapshot");
    }

    public void run() {
        consumer.accept(to);
    }
}

