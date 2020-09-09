package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;
import ru.ibs.dtm.query.calcite.core.service.impl.CalciteDMLQueryParserService;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment.AdqmSchemaExtenderImpl.getExtendedColumns;

@Service("adqmCalciteDMLQueryParserService")
public class AdqmCalciteDMLQueryParserService extends CalciteDMLQueryParserService {
    public AdqmCalciteDMLQueryParserService(
        @Qualifier("adqmCalciteContextProvider") CalciteContextProvider contextProvider,
        @Qualifier("adqmVertx") Vertx vertx
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
        extendedSchema.setId(UUID.randomUUID());
        List<DatamartTable> extendedDatamartClasses = new ArrayList<>();
        logicalSchema.getDatamartTables().forEach(dmClass -> {
            DatamartTable nwTable = new DatamartTable();
            nwTable.setDatamartMnemonic(dmClass.getDatamartMnemonic());
            nwTable.setPrimaryKeys(dmClass.getPrimaryKeys());
            nwTable.setMnemonic(dmClass.getMnemonic());
            nwTable.setLabel(dmClass.getLabel());
            nwTable.setId(UUID.randomUUID());
            nwTable.setTableAttributes(new ArrayList<>(dmClass.getTableAttributes()));
            nwTable.getTableAttributes().addAll(getExtendedColumns());
            extendedDatamartClasses.add(nwTable);
        });
        extendedSchema.setDatamartTables(extendedDatamartClasses);
        return extendedSchema;
    }
}
