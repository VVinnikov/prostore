package ru.ibs.dtm.query.execution.plugin.adb.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adb.model.metadata.Datamart;

import java.util.ArrayList;
import java.util.List;

@Component
public class CalciteContextProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(CalciteContextProvider.class);

  private final List<RelTraitDef> traitDefs;
  private final RuleSet prepareRules;
  private final SqlParser.Config configParser;
  private final CalciteSchemaFactory calciteSchemaFactory;

  @Autowired
  public CalciteContextProvider(@Qualifier("adbParserConfig") SqlParser.Config configParser,
                                CalciteSchemaFactory calciteSchemaFactory) {
    this.configParser = configParser;
    prepareRules =
      RuleSets.ofList(
        EnumerableRules.ENUMERABLE_RULES);

    traitDefs = new ArrayList<>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    this.calciteSchemaFactory = calciteSchemaFactory;
  }

  public CalciteContext context(Datamart defaultDatamart) {
    try {
      final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
      FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(configParser)
        .defaultSchema(
          defaultDatamart == null ? rootSchema :
            calciteSchemaFactory.addSchema(rootSchema, defaultDatamart))
        .traitDefs(traitDefs).programs(
          Programs.of(prepareRules)
        )
        .sqlToRelConverterConfig(SqlToRelConverter.configBuilder().withExpand(false).build())
        .build();
      Planner planner = Frameworks.getPlanner(config);
      return new CalciteContext(rootSchema, planner, RelBuilder.create(config));
    } catch (Exception e) {
      LOGGER.error("Ошибка создания планировщика", e);
    }
    return null;
  }

  public void enrichContext(CalciteContext context, Datamart datamart) {
    calciteSchemaFactory.addSchema(context.getSchema(), datamart);
  }
}
