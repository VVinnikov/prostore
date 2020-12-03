package io.arenadata.dtm.query.execution.plugin.adb.service.impl.check;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Params;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;

@Service("adbCheckDataService")
public class AdbCheckDataService implements CheckDataService {

    private static final String CHECK_DATA_BY_COUNT_TEMPLATE = "SELECT count(1) FROM " +
            "(SELECT 1 " +
            "FROM %s.%s_history " +
            "WHERE (sys_to = %d AND sys_op = 1) OR sys_from = %d" +
            "UNION ALL " +
            "SELECT 1 " +
            "FROM %s.%s_actual " +
            "WHERE sys_from = %d) AS tmp";
    private static final String CREATE_OR_REPLACE_FUNC = "CREATE OR REPLACE FUNCTION dtmInt32Hash(bytea) RETURNS integer\n" +
            "    AS 'select get_byte($1, 0)+(get_byte($1, 1)<<8)+(get_byte($1, 2)<<16)+(get_byte($1, 3)<<24)' \n" +
            "    LANGUAGE SQL\n" +
            "    IMMUTABLE\n" +
            "    LEAKPROOF\n" +
            "    RETURNS NULL ON NULL INPUT;";
    private static final String CHECK_DATA_BY_HASH_TEMPLATE =
            "SELECT sum(dtmInt32Hash(MD5(concat(%s))::bytea)) FROM\n" +
            "(\n" +
            "  SELECT %s \n" +
            "  FROM %s.%s_history \n" +
            "  WHERE (sys_to = %d AND sys_op = 1) OR sys_from = %d \n" +
            "  UNION ALL \n" +
            "  SELECT %s \n" +
            "  FROM %s.%s_actual \n" +
            "  WHERE sys_from = %d\n" +
            ") AS tmp";
    private static final String COLUMN_NAME = "count(1)";
    private final AdbQueryExecutor queryExecutor;

    @Autowired
    public AdbCheckDataService(AdbQueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountParams params) {
        return Future.future(p -> {
            val sql = String.format(CHECK_DATA_BY_COUNT_TEMPLATE,
                    params.getEntity().getSchema(), params.getEntity().getName(),
                    params.getSysCn() -1, params.getSysCn(),
                    params.getEntity().getSchema(), params.getEntity().getName(),
                    params.getSysCn());
            ColumnMetadata metadata = new ColumnMetadata(COLUMN_NAME, ColumnType.BIGINT);
            queryExecutor.execute(sql, Collections.singletonList(metadata), ar -> {
                if (ar.succeeded()) {
                    p.complete(Long.valueOf(ar.result().get(0).get(COLUMN_NAME).toString()));
                } else {
                    p.fail(ar.cause());
                }
            });
        });
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Params params) {
        return createOrReplaceFunction()
                .compose(v -> checkDataByHash(params));
    }

    private Future<Void> createOrReplaceFunction(){
        return Future.future(p -> {
            queryExecutor.executeUpdate(CREATE_OR_REPLACE_FUNC, p);
        });
    }

    private Future<Long> checkDataByHash(CheckDataByHashInt32Params params){
        return Future.future(p -> {
            val columnsList = String.join(",", params.getColumns());
            val datamart = params.getEntity().getSchema();
            val table = params.getEntity().getName();
            val sysCn = params.getSysCn();
            val sql = String.format(CHECK_DATA_BY_HASH_TEMPLATE, columnsList,
                    columnsList,
                    datamart, table,
                    sysCn - 1, sysCn,
                    columnsList,
                    datamart, table,
                    sysCn);
            val columnMetadata = new ColumnMetadata("sum", ColumnType.BIGINT);
            queryExecutor.execute(sql, Collections.singletonList(columnMetadata), ar -> {
                if (ar.succeeded()) {
                    val res = ar.result().get(0).get("sum");
                    if (res == null) {
                        p.complete(0L);
                    } else {
                        p.complete(Long.valueOf(res.toString()));
                    }
                } else {
                    p.fail(ar.cause());
                }
            });
        });
    }
}
