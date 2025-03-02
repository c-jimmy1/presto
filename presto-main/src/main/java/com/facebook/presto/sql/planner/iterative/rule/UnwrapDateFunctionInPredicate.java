package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.analyzer.FunctionAndTypeResolver;
import com.facebook.presto.common.function.OperatorType;
import com.google.common.collect.ImmutableList;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.Optional;

import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

/**
 * A rule that rewrites function-based date predicates into range predicates.
 * For example, it rewrites:
 *   date(timestamp_column) = DATE '2020-10-10'
 * into:
 *   timestamp_column >= TIMESTAMP '2020-10-10 00:00:00.000' AND
 *   timestamp_column <  TIMESTAMP '2020-10-11 00:00:00.000'
 */
public class UnwrapDateFunctionInPredicate
        implements Rule<FilterNode> {
    private static final String DATE_FUNCTION = "date";
    private static final String YEAR_FUNCTION = "year";
    private static final String DATE_TRUNC_FUNCTION = "date_trunc";

    private final FunctionAndTypeManager functionAndTypeManager;
    private final StandardFunctionResolution functionResolution;

    public UnwrapDateFunctionInPredicate(FunctionAndTypeManager functionAndTypeManager) {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<FilterNode> getPattern() {
        return typeOf(FilterNode.class);
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context) {
        RowExpression predicate = node.getPredicate();
        Optional<RowExpression> rewritten = rewritePredicate(predicate);

        if (!rewritten.isPresent()) {
            return Result.empty();
        }

        return Result.ofPlanNode(
                new FilterNode(
                        node.getSourceLocation(),
                        node.getId(),
                        node.getSource(),
                        rewritten.get()));
    }

    private Optional<RowExpression> rewritePredicate(RowExpression expression) {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression call = (CallExpression) expression;
        FunctionResolution functionResolution = new FunctionResolution(StandardFunctionResolution);

        // Check if this is an equality comparison
        if (!functionResolution.isEqualsFunction(call.getFunctionHandle())) {
            return Optional.empty();
        }

        RowExpression left = call.getArguments().get(0);
        RowExpression right = call.getArguments().get(1);

        // Try both function(column) = literal and literal = function(column)
        Optional<RowExpression> rewritten = tryRewriteFunctionEqualsLiteral(left, right);
        if (rewritten.isPresent()) {
            return rewritten;
        }

        return tryRewriteFunctionEqualsLiteral(right, left);
    }

    private Optional<RowExpression> tryRewriteFunctionEqualsLiteral(RowExpression functionSide, RowExpression literalSide) {
        if (!(functionSide instanceof CallExpression) || !(literalSide instanceof ConstantExpression)) {
            return Optional.empty();
        }

        CallExpression function = (CallExpression) functionSide;
        ConstantExpression literal = (ConstantExpression) literalSide;

        // Get function name
        String functionName = function.getDisplayName();

        if (functionName.equalsIgnoreCase(DATE_FUNCTION)) {
            return rewriteDateFunction(function, literal);
        } else if (functionName.equalsIgnoreCase(YEAR_FUNCTION)) {
            return rewriteYearFunction(function, literal);
        } else if (functionName.equalsIgnoreCase(DATE_TRUNC_FUNCTION)) {
            return rewriteDateTruncFunction(function, literal);
        }

        return Optional.empty();
    }

    private Optional<RowExpression> rewriteDateFunction(CallExpression function, ConstantExpression literal) {
        // Ensure this is date(column) = DATE '...'
        if (function.getArguments().size() != 1 || !(literal.getType() instanceof DateType)) {
            return Optional.empty();
        }

        RowExpression column = function.getArguments().get(0);
        if (!(column instanceof VariableReferenceExpression || column instanceof InputReferenceExpression)) {
            return Optional.empty();
        }

        // Extract date from literal
        LocalDate date = (LocalDate) literal.getValue();

        // Create timestamp bounds for the entire day
        LocalDateTime lowerBound = date.atStartOfDay();
        LocalDateTime upperBound = date.plusDays(1).atStartOfDay();

        return Optional.of(createTimestampRangePredicate(column, lowerBound, upperBound));
    }

    private Optional<RowExpression> rewriteYearFunction(CallExpression function, ConstantExpression literal) {
        // Ensure this is year(column) = 2020 (or similar)
        if (function.getArguments().size() != 1 || !(literal.getValue() instanceof Long)) {
            return Optional.empty();
        }

        RowExpression column = function.getArguments().get(0);
        if (!(column instanceof VariableReferenceExpression || column instanceof InputReferenceExpression)) {
            return Optional.empty();
        }

        // Extract year from literal
        long year = (Long) literal.getValue();

        // Create timestamp bounds for the entire year
        LocalDateTime lowerBound = LocalDateTime.of((int) year, 1, 1, 0, 0);
        LocalDateTime upperBound = LocalDateTime.of((int) year + 1, 1, 1, 0, 0);

        return Optional.of(createTimestampRangePredicate(column, lowerBound, upperBound));
    }

    private Optional<RowExpression> rewriteDateTruncFunction(CallExpression function, ConstantExpression literal) {
        // Ensure this is date_trunc('unit', column) = TIMESTAMP '...'
        if (function.getArguments().size() != 2 || !(literal.getType() instanceof TimestampType)) {
            return Optional.empty();
        }

        // First argument should be the truncation unit (as a constant)
        if (!(function.getArguments().get(0) instanceof ConstantExpression)) {
            return Optional.empty();
        }

        ConstantExpression unitLiteral = (ConstantExpression) function.getArguments().get(0);
        if (!(unitLiteral.getValue() instanceof String)) {
            return Optional.empty();
        }

        String unit = (String) unitLiteral.getValue();
        RowExpression column = function.getArguments().get(1);

        if (!(column instanceof VariableReferenceExpression || column instanceof InputReferenceExpression)) {
            return Optional.empty();
        }

        // Extract timestamp from literal
        LocalDateTime timestamp = (LocalDateTime) literal.getValue();

        // Calculate bounds based on truncation unit
        LocalDateTime lowerBound = timestamp;
        LocalDateTime upperBound;

        switch (unit.toLowerCase()) {
            case "year":
                upperBound = timestamp.with(TemporalAdjusters.firstDayOfNextYear());
                break;
            case "month":
                upperBound = timestamp.with(TemporalAdjusters.firstDayOfNextMonth());
                break;
            case "day":
                upperBound = timestamp.plusDays(1);
                break;
            case "hour":
                upperBound = timestamp.plusHours(1);
                break;
            case "minute":
                upperBound = timestamp.plusMinutes(1);
                break;
            default:
                return Optional.empty(); // Unsupported truncation unit
        }

        return Optional.of(createTimestampRangePredicate(column, lowerBound, upperBound));
    }

}