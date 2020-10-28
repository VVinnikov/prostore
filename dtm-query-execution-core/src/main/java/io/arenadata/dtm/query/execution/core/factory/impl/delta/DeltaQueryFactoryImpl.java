package io.arenadata.dtm.query.execution.core.factory.impl.delta;

import io.arenadata.dtm.query.calcite.core.extension.delta.SqlBeginDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlCommitDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlRollbackDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaByDateTime;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaByNum;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaHot;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaOk;
import io.arenadata.dtm.query.execution.core.dto.delta.query.*;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

import static io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER;

@Component
@Slf4j
public class DeltaQueryFactoryImpl implements DeltaQueryFactory {

    private final SqlDialect sqlDialect;

    @Autowired
    public DeltaQueryFactoryImpl(@Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    @Override
    public DeltaQuery create(SqlNode sqlNode) {
        if (sqlNode instanceof SqlBeginDelta) {
            return BeginDeltaQuery.builder()
                    .deltaNum(((SqlBeginDelta) sqlNode).getDeltaNumOperator().getNum())
                    .build();
        } else if (sqlNode instanceof SqlCommitDelta) {
            return CommitDeltaQuery.builder()
                    .deltaDate(getDeltaDateTime(((SqlCommitDelta) sqlNode).getDeltaDateTimeOperator().getDeltaDateTime()))
                    .build();
        } else if (sqlNode instanceof SqlGetDeltaOk) {
            return GetDeltaOkQuery.builder().build();
        } else if (sqlNode instanceof SqlGetDeltaHot) {
            return GetDeltaHotQuery.builder().build();
        } else if (sqlNode instanceof SqlGetDeltaByNum) {
            return GetDeltaByNumQuery.builder()
                    .deltaNum(((SqlGetDeltaByNum) sqlNode).getDeltaNum())
                    .build();
        } else if (sqlNode instanceof SqlGetDeltaByDateTime) {
            return GetDeltaByDateTimeQuery.builder()
                    .deltaDate(getDeltaDateTime(((SqlGetDeltaByDateTime) sqlNode).getDeltaDateTime()))
                    .build();
        } else if (sqlNode instanceof SqlRollbackDelta) {
            return RollbackDeltaQuery.builder().build();
        } else {
            throw new RuntimeException(String.format("Query [%s] is not a DELTA operator",
                    sqlNode.toSqlString(sqlDialect)));
        }
    }

    private LocalDateTime getDeltaDateTime(String deltaDateTimeStr) {
        if (deltaDateTimeStr != null) {
            return LocalDateTime.parse(deltaDateTimeStr, DELTA_DATE_TIME_FORMATTER);
        } else {
            return null;
        }
    }
}
