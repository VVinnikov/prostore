package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service("adqmCheckDataService")
public class AdqmCheckDataService implements CheckDataService {
    private static final String COUNT = "count";
    private static final String COUNT_QUERY_PATTERN = "  SELECT count(1) as %s\n" +
            "  FROM %s__%s.%s_actual FINAL\n" +
            "  WHERE (sys_to = %s AND sys_op = 1) OR sys_from = %s";

    private static final String SUM = "sum";
    private static final String HASH_QUERY_PATTERN = "SELECT \n" +
            "  sum(\n" +
            "    reinterpretAsUInt32(\n" +
            "      lower(\n" +
            "        hex(\n" +
            "          MD5(\n" +
            "            %s\n" +
            "          )\n" +
            "        )\n" +
            "      )\n" +
            "    )\n" +
            "  ) as %s\n" +
            "FROM %s__%s.%s_actual FINAL\n" +
            "WHERE (sys_to = %s AND sys_op = 1) OR sys_from = %s";

    private final DatabaseExecutor adqmQueryExecutor;

    @Autowired
    public AdqmCheckDataService(DatabaseExecutor adqmQueryExecutor) {
        this.adqmQueryExecutor = adqmQueryExecutor;
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        Entity entity = request.getEntity();
        //TODO it's better to exclude generating sql query in separate factory class
        String query = String.format(COUNT_QUERY_PATTERN, COUNT, request.getEnv(), entity.getSchema(),
                entity.getName(), request.getSysCn() - 1, request.getSysCn());
        return adqmQueryExecutor.execute(query)
                .map(result -> Long.parseLong(result.get(0).get(COUNT).toString()));
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request request) {
        Entity entity = request.getEntity();
        List<String> columns = request.getColumns().stream()
                .map(column -> String.format("ifNull(toString(%s),'')", column))
                .collect(Collectors.toList());
        String colQuery = columns.size() > 1
                ? String.format("concat(%s)", String.join(",';',", columns))
                : columns.stream().findFirst().get();//TODO refactor this

        String query = String.format(HASH_QUERY_PATTERN, colQuery, SUM, request.getEnv(), entity.getSchema(),
                entity.getName(), request.getSysCn() - 1, request.getSysCn());
        return adqmQueryExecutor.execute(query)
                .map(result -> Long.parseLong(result.get(0).get(SUM).toString()));
    }
}
