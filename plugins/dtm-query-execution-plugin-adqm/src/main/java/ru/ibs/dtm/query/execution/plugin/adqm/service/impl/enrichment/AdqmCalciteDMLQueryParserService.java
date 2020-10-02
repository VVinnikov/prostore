package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.Vertx;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;
import ru.ibs.dtm.query.calcite.core.service.impl.CalciteDMLQueryParserService;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment.AdqmSchemaExtenderImpl.getExtendedColumns;

@Service("adqmCalciteDMLQueryParserService")
public class AdqmCalciteDMLQueryParserService extends CalciteDMLQueryParserService {
    public AdqmCalciteDMLQueryParserService(
        @Qualifier("adqmCalciteContextProvider") CalciteContextProvider contextProvider,
        @Qualifier("coreVertx") Vertx vertx
    ) {
        super(contextProvider, vertx);
    }

    @Override
    protected List<Datamart> extendSchemes(List<Datamart> datamarts) {
        return super.extendSchemes(datamarts.stream()
            .map(this::withSystemFields)
            .collect(Collectors.toList()));
    }

    private Datamart withSystemFields(Datamart logicalSchema) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(logicalSchema.getMnemonic());
        List<Entity> extendedDatamartClasses = new ArrayList<>();
        logicalSchema.getEntities().forEach(entity -> {
            val extendedFields = new ArrayList<>(entity.getFields());
            extendedFields.addAll(getExtendedColumns());
            extendedDatamartClasses.add(entity.toBuilder()
                .fields(extendedFields)
                .build());
        });
        extendedSchema.setEntities(extendedDatamartClasses);
        return extendedSchema;
    }
}
