package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.core.service.dml.InformationSchemaDefinitionService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class InformationSchemaDefinitionServiceImpl implements InformationSchemaDefinitionService {
    private static final String INFORMATION_SCHEMA = "information_schema";
    private static final String LOGIC_PREFIX = "logic_";

    @Override
    public boolean isInformationSchemaRequest(List<DeltaInformation> deltaInformations) {
        Set<String> unicSchemes = deltaInformations.stream()
                .map(DeltaInformation::getSchemaName)
                .map(String::toUpperCase)
                .collect(Collectors.toSet());

        boolean informationSchemaExists = unicSchemes.contains(InformationSchemaView.SCHEMA_NAME);

        if (unicSchemes.size() > 1 && informationSchemaExists) {
            throw new DtmException("Simultaneous query to the information schema and user schema isn't supported");
        } else {
            return informationSchemaExists;
        }
    }

    @Override
    public Future<Void> checkAccessToSystemLogicalTables(SqlNode query) {
        return Future.future(p -> {
            boolean existsSystemLogicTable = new SqlSelectTree(query).findAllTableAndSnapshots().stream()
                    .filter(tableNode -> INFORMATION_SCHEMA.equalsIgnoreCase(tableNode.tryGetSchemaName().orElse("")))
                    .map(tableNode -> tableNode.tryGetTableName().orElseThrow(() -> new DtmException("Can't get table name")))
                    .anyMatch(tableName -> tableName.toLowerCase().startsWith(LOGIC_PREFIX));
            if (existsSystemLogicTable) {
                p.fail(new DtmException("Access to system logical tables is forbidden"));
            } else {
                p.complete();
            }
        });
    }
}
