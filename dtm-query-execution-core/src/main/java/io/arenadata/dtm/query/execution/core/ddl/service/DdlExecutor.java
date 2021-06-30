package io.arenadata.dtm.query.execution.core.ddl.service;

import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public interface DdlExecutor<T> {

    Future<T> execute(DdlRequestContext context, String sqlNodeName);

    Set<SqlKind> getSqlKinds();

    default List<PostSqlActionType> getPostActions() {
        return Arrays.asList(PostSqlActionType.PUBLISH_STATUS, PostSqlActionType.UPDATE_INFORMATION_SCHEMA);
    }
}
