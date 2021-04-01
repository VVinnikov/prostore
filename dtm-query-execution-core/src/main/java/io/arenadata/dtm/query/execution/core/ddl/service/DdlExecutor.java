package io.arenadata.dtm.query.execution.core.ddl.service;

import io.arenadata.dtm.common.post.PostSqlActionType;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;

public interface DdlExecutor<T> {

    Future<T> execute(DdlRequestContext context, String sqlNodeName);

    SqlKind getSqlKind();

    default List<PostSqlActionType> getPostActions() {
        return Arrays.asList(PostSqlActionType.PUBLISH_STATUS, PostSqlActionType.UPDATE_INFORMATION_SCHEMA);
    }

    @Autowired
    default void register(DdlService<T> service) {
        service.addExecutor(this);
    }

}
