package ru.ibs.dtm.query.calcite.core.node;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.*;

@Data
@Slf4j
public class SqlSelectTree {
    public final static String SELECT_AS = "SELECT.AS";
    public final static String SELECT_AS_IDENTIFIER = "SELECT.AS.IDENTIFIER";
    public final static String SELECT_IDENTIFIER = "SELECT.IDENTIFIER";
    public final static String SELECT_AS_SNAPSHOT = "SNAPSHOT";
    private final Map<Integer, SqlTreeNode> nodeMap;
    private int idCounter;

    public SqlSelectTree(SqlNode sqlSelect) {
        nodeMap = new TreeMap<>();
        createRoot(sqlSelect).ifPresent(this::addNodes);
    }

    public SqlTreeNode getRoot() {
        return nodeMap.get(0);
    }

    public Optional<SqlTreeNode> getParentByChild(SqlTreeNode child) {
        return Optional.ofNullable(nodeMap.get(child.getParentId()));
    }

    public List<SqlTreeNode> findSnapshots() {
        return findNodesByPath(SELECT_AS_SNAPSHOT);
    }

    public List<SqlTreeNode> findNodesByPath(String pathPostfix) {
        return filterChild(nodeMap.values().stream()
                .filter(n -> n.getKindPath().endsWith(pathPostfix))
                .collect(Collectors.toList()));
    }

    public List<SqlTreeNode> findNodes(Predicate<SqlTreeNode> predicate) {
        return filterChild(nodeMap.values().stream()
                .filter(predicate)
                .collect(Collectors.toList()));
    }

    private List<SqlTreeNode> filterChild(List<SqlTreeNode> nodeList) {
        return nodeList.stream()
                .filter(n1 -> nodeList.stream().noneMatch(n2 -> n1.getParentId() == n2.getId()))
                .collect(Collectors.toList());
    }

    private void flattenSql(SqlTreeNode treeNode) {
        val node = treeNode.getNode();
        if (node instanceof SqlSelect) {
            flattenSqlSelect(treeNode, (SqlSelect) node);
        } else if (node instanceof SqlNodeList) {
            flattenSqlNodeList(treeNode, (SqlNodeList) node);
        } else if (node instanceof SqlJoin) {
            flattenSqlJoin(treeNode, (SqlJoin) node);
        } else if (node instanceof SqlIdentifier) {
            flattenSqlIdentifier(treeNode, (SqlIdentifier) node);
        } else if (node instanceof SqlSnapshot) {
            flattenSqlSnapshot(treeNode, (SqlSnapshot) node);
        } else if (node instanceof SqlBasicCall) {
            flattenSqlBasicCall(treeNode, (SqlBasicCall) node);
        } else if (node instanceof SqlCall) {
            flattenSqlCall(treeNode, (SqlCall) node);
        }
    }

    private void flattenSqlBasicCall(SqlTreeNode parentTree, SqlBasicCall parentNode) {
        flattenSqlCall(parentTree, parentNode);
    }

    private void flattenSqlSnapshot(SqlTreeNode parent, SqlSnapshot parentNode) {
        flattenSqlCall(parent, parentNode);
    }

    private void flattenSqlIdentifier(SqlTreeNode parentTree, SqlIdentifier parentNode) {
        parentTree.setIdentifier(true);
    }

    private void flattenSqlCall(SqlTreeNode parentTree, SqlCall parentNode) {
        val nodes = parentNode.getOperandList();
        for (int i = 0; i < nodes.size(); i++) {
            val itemNode = nodes.get(i);
            int finalI = i;
            parentTree.createChild(idCounter++, itemNode, n -> parentNode.setOperand(finalI, n)).ifPresent(this::addNodes);
        }
    }

    private void flattenSqlSelect(SqlTreeNode parentTree, SqlSelect parentNode) {
        parentTree.createChild(idCounter++,
                parentNode.getFrom(),
                parentNode::setFrom)
                .ifPresent(this::addNodes);
        parentTree.createChild(idCounter++,
                parentNode.getSelectList(),
                sqlNode -> parentNode.setSelectList((SqlNodeList) sqlNode))
                .ifPresent(this::addNodes);
        parentTree.createChild(idCounter++, parentNode.getWhere(), parentNode::setWhere)
                .ifPresent(this::addNodes);
    }

    private void flattenSqlJoin(SqlTreeNode parentTree, SqlJoin parentNode) {
        parentTree.createChild(idCounter++, parentNode.getLeft(), parentNode::setLeft).ifPresent(this::addNodes);
        parentTree.createChild(idCounter++, parentNode.getRight(), parentNode::setRight).ifPresent(this::addNodes);
    }

    private void flattenSqlNodeList(SqlTreeNode parentTree, SqlNodeList parentNode) {
        val nodes = parentNode.getList();
        for (int i = 0; i < nodes.size(); i++) {
            val itemNode = nodes.get(i);
            int finalI = i;
            parentTree.createChild(idCounter++, itemNode, n -> parentNode.set(finalI, n)).ifPresent(this::addNodes);
        }
    }

    public List<SqlTreeNode> getTableOrSnapshots() {
        return this.findNodes(this::isTableOrSnapshot).stream()
                .collect(Collectors.groupingBy(SqlTreeNode::getParentId))
                .values().stream()
                .map(l -> l.get(0))
                .sorted(Comparator.comparing(SqlTreeNode::getId))
                .collect(Collectors.toList());
    }

    private boolean isTableOrSnapshot(SqlTreeNode n) {
        boolean isSnapshot = n.getNode() instanceof SqlSnapshot;
        return isSnapshot || isTable(n);
    }

    private boolean isTable(SqlTreeNode n) {
        String kindPath = n.getKindPath();
        return kindPath.endsWith(SELECT_AS_IDENTIFIER) || kindPath.endsWith(SELECT_IDENTIFIER);
    }

    private void addNodes(SqlTreeNode... nodes) {
        for (val node : nodes) {
            nodeMap.put(node.getId(), node);
            flattenSql(node);
        }
    }

    private Optional<SqlTreeNode> createRoot(SqlNode node) {
        return Optional.of(new SqlTreeNode(idCounter++,
                -1,
                0,
                node,
                null,
                node.getKind().toString()));
    }
}
