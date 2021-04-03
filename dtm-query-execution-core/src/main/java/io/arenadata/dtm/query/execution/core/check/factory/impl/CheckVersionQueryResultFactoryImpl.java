package io.arenadata.dtm.query.execution.core.check.factory.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.core.check.factory.CheckVersionQueryResultFactory;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class CheckVersionQueryResultFactoryImpl implements CheckVersionQueryResultFactory {

    public static final String COMPONENT_NAME_COLUMN = "component_name";
    public static final String VERSION_COLUMN = "version";

    @Override
    public QueryResult create(List<VersionInfo> versionInfos) {
        return QueryResult.builder()
                .metadata(getMetadata())
                .result(createResultList(versionInfos))
                .build();
    }

    private List<ColumnMetadata> getMetadata() {
        return Arrays.asList(
                ColumnMetadata.builder().name(COMPONENT_NAME_COLUMN).type(ColumnType.VARCHAR).build(),
                ColumnMetadata.builder().name(VERSION_COLUMN).type(ColumnType.VARCHAR).build()
        );
    }

    private List<Map<String, Object>> createResultList(List<VersionInfo> versionInfos) {
        List<Map<String, Object>> result = new ArrayList<>();
        versionInfos.forEach(v -> {
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put(COMPONENT_NAME_COLUMN, v.getName());
            resultMap.put(VERSION_COLUMN, v.getVersion());
            result.add(resultMap);
        });
        return result;
    }
}
