package ru.ibs.dtm.query.execution.plugin.adqm.calcite;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;

public class CalciteContext {
    private SchemaPlus schema;
    private Planner planner;
    private RelBuilder relBuilder;

    public CalciteContext(SchemaPlus schema, Planner planner, RelBuilder relBuilder) {
        this.schema = schema;
        this.planner = planner;
        this.relBuilder = relBuilder;
    }

    public CalciteContext(SchemaPlus schema, Planner planner) {
        this.schema = schema;
        this.planner = planner;
    }

    public SchemaPlus getSchema() {
        return schema;
    }

    public Planner getPlanner() {
        return planner;
    }

    public void setSchema(SchemaPlus schema) {
        this.schema = schema;
    }

    public RelBuilder getRelBuilder() {
        return relBuilder;
    }
}
