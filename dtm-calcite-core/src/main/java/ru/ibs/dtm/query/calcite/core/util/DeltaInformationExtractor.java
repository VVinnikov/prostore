package ru.ibs.dtm.query.calcite.core.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.delta.DeltaInformationResult;
import ru.ibs.dtm.query.calcite.core.node.SqlSelectTree;
import ru.ibs.dtm.query.calcite.core.node.SqlTreeNode;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.*;

@Slf4j
public class DeltaInformationExtractor {
    private static final SqlDialect DIALECT = new SqlDialect(CalciteSqlDialect.EMPTY_CONTEXT);
    private static final DateTimeFormatter LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendLiteral("'")
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .appendLiteral("'")
            .toFormatter();

    public static DeltaInformationResult extract(SqlNode root, String defaultDatamart) {
        try {
            val tree = new SqlSelectTree(root);
            val nodes = tree.findTableOrSnapshots();
            nodes.forEach(n -> setDatamart(n, defaultDatamart));
            val deltaInformations = getDeltaInformations(tree, nodes);
            replaceSnapshots(getSnapshots(nodes));
            return new DeltaInformationResult(deltaInformations, root.toSqlString(DIALECT).toString());
        } catch (Exception e) {
            log.error("DeltaInformation extracts Error", e);
            throw e;
        }
    }

    private static void setDatamart(SqlTreeNode n, String defaultDatamart) {
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

    private static void setDatamartToIdentifier(SqlTreeNode n, String defaultDatamart) {
        SqlIdentifier identifier = getSqlIdentifier(defaultDatamart, n.getNode());
        n.getSqlNodeSetter().accept(identifier);
    }

    private static SqlIdentifier getSqlIdentifier(String defaultDatamart, SqlIdentifier node) {
        SqlIdentifier identifier = node;
        if (identifier.isSimple()) {
            identifier = new SqlIdentifier(Arrays.asList(defaultDatamart, identifier.getSimple()),
                    identifier.getParserPosition());
        }
        return identifier;
    }

    private static List<DeltaInformation> getDeltaInformations(SqlSelectTree tree, List<SqlTreeNode> nodes) {
        return nodes.stream()
                .map(n -> getDeltaInformation(tree, n))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static List<SqlTreeNode> getSnapshots(List<SqlTreeNode> nodes) {
        return nodes.stream()
                .filter(n -> n.getNode() instanceof SqlSnapshot)
                .collect(Collectors.toList());
    }

    private static void replaceSnapshots(List<SqlTreeNode> snapshots) {
        for (int i = snapshots.size() - 1; i >= 0; i--) {
            val snapshot = snapshots.get(i);
            SqlSnapshot nodeSqlSnapshot = snapshot.getNode();
            snapshot.getSqlNodeSetter().accept(nodeSqlSnapshot.getTableRef());
        }
    }

    private static DeltaInformation getDeltaInformation(SqlSelectTree tree, SqlTreeNode n) {
        Optional<SqlTreeNode> optParent = tree.getParentByChild(n);
        if (optParent.isPresent()) {
            SqlTreeNode parent = optParent.get();
            if (parent.getNode() instanceof SqlBasicCall) {
                return fromSqlBasicCall(parent.getNode());
            }
        }
        if (n.getNode() instanceof SqlIdentifier) {
            return fromIdentifier(n.getNode(), null, null, null);
        }
        if (n.getNode() instanceof SqlBasicCall) {
            return fromIdentifier(n.getNode(), null, null, null);
        } else {
            return fromSnapshot(n.getNode(), null);
        }
    }

    private static DeltaInformation fromSqlBasicCall(SqlBasicCall basicCall) {
        DeltaInformation deltaInformation = null;
        if (basicCall.getKind() == SqlKind.AS) {
            if (basicCall.operands.length != 2) {
                log.warn("Suspicious AS relation {}", basicCall);
            } else {
                SqlNode left = basicCall.operands[0];
                SqlNode right = basicCall.operands[1];
                if (!(right instanceof SqlIdentifier)) {
                    log.warn("Expecting Sql;Identifier as alias, got {}", right);
                } else if (left instanceof SqlSnapshot) {
                    SqlIdentifier newId = (SqlIdentifier) ((SqlSnapshot) left).getTableRef();
                    basicCall.operands[0] = newId;
                    deltaInformation = fromSnapshot((SqlSnapshot) left, (SqlIdentifier) right);
                } else if (left instanceof SqlIdentifier) {
                    deltaInformation = fromIdentifier((SqlIdentifier) left, (SqlIdentifier) right, null, null);
                }
            }
        }
        return deltaInformation;
    }


    private static DeltaInformation fromSnapshot(SqlSnapshot snapshot, SqlIdentifier alias) {
        val snapshotTime = snapshot.getPeriod().toString().trim();
        return fromIdentifier((SqlIdentifier) snapshot.getTableRef(), alias, snapshotTime, snapshot.getParserPosition());
    }

    private static DeltaInformation fromIdentifier(SqlIdentifier id,
                                                   SqlIdentifier alias,
                                                   String snapshotTime,
                                                   SqlParserPos pos) {
        String datamart = "";
        String tableName;
        if (id.names.size() > 1) {
            datamart = id.names.get(0);
            tableName = id.names.get(1);
        } else {
            tableName = id.names.get(0);
        }

        String aliasVal = "";
        if (alias != null) {
            aliasVal = alias.names.get(0);
        }

        String deltaTime = snapshotTime == null ? LOCAL_DATE_TIME.format(LocalDateTime.now()) : snapshotTime;

        return new DeltaInformation(
                aliasVal,
                deltaTime,
                0L,
                datamart,
                tableName,
                pos == null ? id.getParserPosition() : pos);
    }
}
