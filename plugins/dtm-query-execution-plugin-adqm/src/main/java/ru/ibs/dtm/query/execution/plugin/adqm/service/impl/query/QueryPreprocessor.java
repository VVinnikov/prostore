package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.calcite.CalciteContext;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.DeltaInformation;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.*;

@Component
@Slf4j
public class QueryPreprocessor {
    private static final DateTimeFormatter LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .toFormatter();

    private final CalciteContextProvider calciteContextProvider;

    public QueryPreprocessor(CalciteContextProvider calciteContextProvider) {
        this.calciteContextProvider = calciteContextProvider;
    }

    public void process(String sql, Handler<AsyncResult<List<DeltaInformation>>> handler) {
        CalciteContext context = calciteContextProvider.context(null);
        try {
            SqlNode root = context.getPlanner().parse(sql);
            // FIXME support SqlOrderBy, SqlUnion which contains SqlSelect itself
            // FIXME support for `select a, (select b from t) as b from t2`
            if (!(root instanceof SqlSelect)) {
                handler.handle(Future.failedFuture("Expecting SELECT to extract information"));
                return;
            }
            List<DeltaInformation> result = new ArrayList<>();
            processFrom(((SqlSelect) root).getFrom(), result);
            handler.handle(Future.succeededFuture(result));
        } catch (SqlParseException e) {
            handler.handle(Future.failedFuture(e));
        }
    }

    private void processFrom(SqlNode from, List<DeltaInformation> accum) {
        if (from instanceof SqlSelect) {
            processFrom(((SqlSelect) from).getFrom(), accum);
        }

        if (from instanceof SqlBasicCall) {
           SqlKind kind = from.getKind();
           if (kind == SqlKind.AS) {
                if (((SqlBasicCall) from).operands.length != 2) {
                    log.warn("Suspicious AS relation {}", from);
                    return;
                }

                SqlNode left = ((SqlBasicCall) from).operands[0];
                SqlNode right = ((SqlBasicCall) from).operands[1];

                if (!(right instanceof SqlIdentifier)) {
                    log.warn("Expecting Sql;Identifier as alias, got {}", right);
                    return;
                }

               if (left instanceof SqlSelect) {
                   log.debug("Going to the next level, {}", left);
                   processFrom(((SqlSelect) left).getFrom(), accum);
               }

               if (left instanceof SqlSnapshot) {
                   SqlIdentifier newId = (SqlIdentifier) ((SqlSnapshot) left).getTableRef();
                   ((SqlBasicCall) from).operands[0] = newId;
                   accum.add(fromSnapshot((SqlSnapshot) left, (SqlIdentifier) right));
               }

               if (left instanceof SqlIdentifier) {
                   accum.add(fromIdentifier((SqlIdentifier) left, (SqlIdentifier) right, null));
               }
           }

        }

        if (from instanceof SqlJoin) {
            processFrom(((SqlJoin) from).getLeft(), accum);
            processFrom(((SqlJoin) from).getRight(), accum);
        }

        if (from instanceof SqlIdentifier) {
            accum.add(fromIdentifier((SqlIdentifier) from, null, null));
        }

        if (from instanceof SqlSnapshot) {
            accum.add(fromSnapshot((SqlSnapshot) from, null));
        }
    }

    private DeltaInformation fromSnapshot(SqlSnapshot snapshot, SqlIdentifier alias) {
        String snapshotTime = snapshot.getPeriod().toString().replaceAll("'", "");
        return fromIdentifier((SqlIdentifier) snapshot.getTableRef(), alias, snapshotTime);
    }

    private DeltaInformation fromIdentifier(SqlIdentifier id, SqlIdentifier alias, String snapshotTime) {
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

        return new DeltaInformation(datamart, tableName, aliasVal, deltaTime, 0L);
    }
}
