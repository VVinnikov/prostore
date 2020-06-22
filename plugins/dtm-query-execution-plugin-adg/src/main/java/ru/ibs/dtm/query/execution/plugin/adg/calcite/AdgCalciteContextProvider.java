package ru.ibs.dtm.query.execution.plugin.adg.calcite;

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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.calcite.CalciteContext;

import java.util.ArrayList;
import java.util.List;

@Component
public class AdgCalciteContextProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdgCalciteContextProvider.class);
  final private List<RelTraitDef> traitDefs;
  final private RuleSet prepareRules;
  final private SqlParser.Config configParser;

  public AdgCalciteContextProvider(@Qualifier("adgParserConfig") SqlParser.Config configParser) {
    this.configParser = configParser;
    prepareRules =
      RuleSets.ofList(
        EnumerableRules.ENUMERABLE_RULES);

    traitDefs = new ArrayList<>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
  }

  public CalciteContext context() {
    try {
      final SchemaPlus emptySchema = Frameworks.createRootSchema(true);
      FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(configParser)
        .defaultSchema(emptySchema)
        .traitDefs(traitDefs).programs(
          Programs.of(prepareRules)
        )
        .sqlToRelConverterConfig(SqlToRelConverter.configBuilder().withExpand(false).build())
        .build();
      Planner planner = Frameworks.getPlanner(config);
      return new CalciteContext(emptySchema, planner, RelBuilder.create(config));
    } catch (Exception e) {
      LOGGER.error("Ошибка создания планировщика", e);
    }
    return null;
  }
}
