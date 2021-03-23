package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.query.execution.core.calcite.CoreCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.core.dto.dml.LlrRequestContext;
import io.arenadata.dtm.query.execution.core.service.dml.ParametersTypeExtractor;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class ParametersTypeExtractorImpl implements ParametersTypeExtractor {

    private final CoreCalciteDMLQueryParserService parserService;

    @Autowired
    public ParametersTypeExtractorImpl(CoreCalciteDMLQueryParserService parserService) {
        this.parserService = parserService;
    }

    @Override
    public Future<List<SqlTypeName>> extract(LlrRequestContext llrRequestContext) {
        val parserRequest = new QueryParserRequest(llrRequestContext.getOriginalQuery(), llrRequestContext.getQueryTemplateValue().getLogicalSchema());
        List<SqlTypeName> list = new ArrayList<>();
        return parserService.parse(parserRequest)
                .map(response -> {
                    getParamsFromRelNode(response.getRelNode().rel, list);
                    return list;
                });

    }

    private RelNode getParamsFromRelNode(RelNode relNode, List<SqlTypeName> list) {
        return getParamsFromRelNodeInChildren(relNode, list)
                .accept(new RelShuttleImpl() {
                    @Override
                    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
                            getParamsFromRelNodeInChildren(child, list);
                            return super.visitChild(parent, i, child);
                    }
                });
    }

    private RelNode getParamsFromRelNodeInChildren(RelNode rel, List<SqlTypeName> list) {
//        return rel.accept(new RelHomogeneousShuttle() {

//            @Override
//            public RelNode visit(LogicalFilter filter) {
//                return super.visit(filter);
//            }
//
//            @Override
//            public RelNode visit(LogicalJoin join) {
//                val condition = join.getCondition();
//                switch (condition.getType()) {
//                    case EQUALS:
//                    case NOT_EQUALS:
//                    case GREATER_THAN_OR_EQUAL:
//                    case GREATER_THAN:
//                    case LESS_THAN_OR_EQUAL:
//                    case LESS_THAN:
//                        val firstOperand = call.getOperands().get(0);
//                        val secondOperand = call.getOperands().get(1);
////                        list.add(isDateTimeNode(firstOperand, secondOperand));
////                        if (isDateTimeNode(firstOperand, secondOperand)) {
//                            RexNode columnOperator;
//                            if (firstOperand instanceof RexInputRef) {
//                                columnOperator = firstOperand;
//                            } else if (secondOperand instanceof RexInputRef) {
//                                columnOperator = secondOperand;
//                            } else {
//                                return super.visitCall(call);
//                            }
//                        list.add(columnOperator.getType().getSqlTypeName());
////                            list.add(QueryDateTimeCondition.builder()
////                                    .isDateTime(isDateTimeNode(firstOperand, secondOperand))
////                                    .type(columnOperator.getType().getSqlTypeName())
////                                    .build());
////                            list.add(QueryDateTimeCondition.builder()
////                                    .index(Integer.valueOf(columnOperator.toString().replace("$","")))
////                                    .value(((RexCall) valueOperand).getOperands().get(0).toString())
////                                    .type(columnOperator.getType().getSqlTypeName())
////                                    .sqlKind(operatorKind)
////                                    .build());
//                        return super.visitCall(call);
////                        }
//                    default:
//                        return super.visitCall(call);
//                }
//            }
//                return super.visit(join);
//            }
//        });
        return rel.accept(new RexShuttle() {

            @SneakyThrows
            @Override
            public RexNode visitCall(RexCall call) {
                val operatorKind = call.getOperator().getKind();
                switch (operatorKind) {
                    case EQUALS:
                    case NOT_EQUALS:
                    case GREATER_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case LESS_THAN:
                        val firstOperand = call.getOperands().get(0);
                        val secondOperand = call.getOperands().get(1);
//                        list.add(isDateTimeNode(firstOperand, secondOperand));
//                        if (isDateTimeNode(firstOperand, secondOperand)) {
                            RexNode columnOperator;
                            if (firstOperand instanceof RexInputRef) {
                                columnOperator = firstOperand;
                            } else if (secondOperand instanceof RexInputRef) {
                                columnOperator = secondOperand;
                            } else {
                                return super.visitCall(call);
                            }
                        list.add(columnOperator.getType().getSqlTypeName());
//                            list.add(QueryDateTimeCondition.builder()
//                                    .isDateTime(isDateTimeNode(firstOperand, secondOperand))
//                                    .type(columnOperator.getType().getSqlTypeName())
//                                    .build());
//                            list.add(QueryDateTimeCondition.builder()
//                                    .index(Integer.valueOf(columnOperator.toString().replace("$","")))
//                                    .value(((RexCall) valueOperand).getOperands().get(0).toString())
//                                    .type(columnOperator.getType().getSqlTypeName())
//                                    .sqlKind(operatorKind)
//                                    .build());
                        return super.visitCall(call);
//                        }
                    default:
                        return super.visitCall(call);
                }
            }
        });
    }



    private boolean isDateTimeNode(RexNode firstOperand, RexNode secondOperand) {
        if (isDateTimeType(firstOperand) && isDateTimeType(secondOperand)) {
            if (firstOperand instanceof RexInputRef && secondOperand instanceof RexInputRef) {
                return false;
            } else if (firstOperand instanceof RexInputRef) {
                return true;
            } else if (secondOperand instanceof RexInputRef) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private boolean isDateTimeType(RexNode rexNode) {
        switch (rexNode.getType().getSqlTypeName()) {
            case TIME:
            case DATE:
            case TIMESTAMP:
                return true;
            default:
                return false;
        }
    }
}
