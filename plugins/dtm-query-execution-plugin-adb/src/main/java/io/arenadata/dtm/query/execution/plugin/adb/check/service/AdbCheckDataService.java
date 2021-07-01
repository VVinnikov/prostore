package io.arenadata.dtm.query.execution.plugin.adb.check.service;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbCheckDataQueryFactory;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Collections;

@Service("adbCheckDataService")
public class AdbCheckDataService implements CheckDataService {

    private final AdbCheckDataQueryFactory checkDataFactory;
    private final DatabaseExecutor queryExecutor;
    private static final String COUNT_COLUMN_NAME = "cnt";
    private static final String HASH_SUM_COLUMN_NAME = "hash_sum";

    @Autowired
    public AdbCheckDataService(AdbCheckDataQueryFactory checkDataFactory,
                               @Qualifier("adbQueryExecutor") DatabaseExecutor queryExecutor) {
        this.checkDataFactory = checkDataFactory;
        this.queryExecutor = queryExecutor;
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        return Future.future(p -> {
            ColumnMetadata metadata = new ColumnMetadata(COUNT_COLUMN_NAME, ColumnType.BIGINT);
            queryExecutor.execute(checkDataFactory.createCheckDataByCountQuery(request, COUNT_COLUMN_NAME),
                    Collections.singletonList(metadata))
                    .onSuccess(result -> {
                        p.complete(Long.valueOf(result.get(0).get(COUNT_COLUMN_NAME).toString()));
                    })
                    .onFailure(p::fail);
        });
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request request) {
        return checkDataByHash(request);
    }

    private Future<Long> checkDataByHash(CheckDataByHashInt32Request request) {
        return Future.future(p -> {
            val columnMetadata = new ColumnMetadata(HASH_SUM_COLUMN_NAME, ColumnType.BIGINT);
            queryExecutor.execute(checkDataFactory.createCheckDataByHashInt32Query(request, HASH_SUM_COLUMN_NAME),
                    Collections.singletonList(columnMetadata))
                    .onSuccess(result -> {
                        val res = result.get(0).get(HASH_SUM_COLUMN_NAME);
                        if (res == null) {
                            p.complete(0L);
                        } else {
                            p.complete(Long.valueOf(res.toString()));
                        }
                    })
                    .onFailure(p::fail);
        });
    }
}
