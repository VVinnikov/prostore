package ru.ibs.dtm.query.calcite.core.node;

import java.util.Optional;
import java.util.function.Consumer;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

@Data
public class SqlTreeNode implements Comparable<SqlTreeNode> {
    private final int id;
    private final int parentId;
    private final int level;
    private final SqlNode node;
    private final Consumer<SqlNode> sqlNodeSetter;
    private final String kindPath;
    private boolean identifier;
    private int childPos;

    public Optional<SqlTreeNode> createChild(int id, SqlNode node, Consumer<SqlNode> sqlNodeSetter) {
        if (node == null) return Optional.empty();
        return Optional.of(
                new SqlTreeNode(
                        id,
                        this.id,
                        level + 1,
                        node,
                        sqlNodeSetter,
                        getKindPath(node))
        );
    }

    private String getKindPath(SqlNode node) {
        String childPos = this.childPos > 0 ? "[" + this.childPos + "]" : "";
        this.childPos++;
        return (this.kindPath == null ? "" : this.kindPath) + childPos + "." + node.getKind();
    }

    public void resetChildPos() {
        childPos = 0;
    }

    @SuppressWarnings("unchecked")
    public <T extends SqlNode> T getNode() {
        return (T) node;
    }

    @Override
    public int compareTo(SqlTreeNode o) {
        return Integer.compare(this.getId(), o.getId());
    }
}