package ru.ibs.dtm.query.execution.core.utils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSnapshot;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.calcite.core.node.SqlSelectTree;
import ru.ibs.dtm.query.calcite.core.node.SqlTreeNode;

import java.util.Arrays;

@Component
public class DefaultDatamartSetter {

    public SqlNode set(SqlNode sqlNode, String datamart) {
        SqlSelectTree selectTree = new SqlSelectTree(sqlNode);
        selectTree.findAllTableAndSnapshots().forEach(n -> setDatamart(n, datamart));
        return sqlNode;
    }

    private void setDatamart(SqlTreeNode n, String defaultDatamart) {
        if (n.getNode() instanceof SqlSnapshot) {
            SqlSnapshot snapshot = n.getNode();
            if (snapshot.getTableRef() instanceof SqlIdentifier) {
                SqlIdentifier identifier = (SqlIdentifier) snapshot.getTableRef();
                snapshot.setOperand(0, getSqlIdentifier(defaultDatamart, identifier));
            }
        } else if (n.getNode() instanceof SqlIdentifier) {
            setDatamartToIdentifier(n, defaultDatamart);
        }
    }

    private void setDatamartToIdentifier(SqlTreeNode n, String defaultDatamart) {
        SqlIdentifier identifier = getSqlIdentifier(defaultDatamart, n.getNode());
        n.getSqlNodeSetter().accept(identifier);
    }

    private SqlIdentifier getSqlIdentifier(String defaultDatamart, SqlIdentifier node) {
        SqlIdentifier identifier = node;
        if (identifier.isSimple()) {
            identifier = new SqlIdentifier(Arrays.asList(defaultDatamart, identifier.getSimple()),
                    identifier.getParserPosition());
        }
        return identifier;
    }
}
