package ru.ibs.dtm.query.execution.core.dao.eddl.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.query.execution.core.dao.eddl.DownloadExtTableAttributeDao;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExternalTableAttribute;

import java.util.ArrayList;
import java.util.List;

import static org.jooq.generated.dtmservice.Tables.DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE;

@Repository
public class DownloadExtTableAttributeDaoImpl implements DownloadExtTableAttributeDao {

    private final AsyncClassicGenericQueryExecutor executor;

    @Autowired
    public DownloadExtTableAttributeDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void findDownloadExtTableAttributes(Long detId, Handler<AsyncResult<List<DownloadExternalTableAttribute>>> resultHandler) {
        executor.query(dsl -> dsl
                .select(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.COLUMN_NAME
                        , DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DET_ID
                        , DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DATA_TYPE
                        , DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.ORDER_NUM
                )
                .from(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE)
                .where(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DET_ID.eq(detId))
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                QueryResult result = ar.result();
                ResultSet resultSet = result.unwrap();
                val tableAttributes = new ArrayList<DownloadExternalTableAttribute>();
                resultSet.getRows().forEach(row -> {
                    tableAttributes.add(
                            new DownloadExternalTableAttribute(row.getString(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.COLUMN_NAME.getName()),
                                    row.getString(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DATA_TYPE.getName()),
                                    row.getInteger(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.ORDER_NUM.getName()),
                                    row.getLong(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DET_ID.getName())));
                });
                resultHandler.handle(Future.succeededFuture(tableAttributes));
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void dropDownloadExtTableAttributesByTableId(Long downloadExtTableId, Handler<AsyncResult<Integer>> handler) {
        executor.execute(dsl -> dsl.deleteFrom(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE)
                .where(DOWNLOAD_EXTERNAL_TABLE_ATTRIBUTE.DET_ID.eq(downloadExtTableId)))
                .setHandler(handler);
    }
}
