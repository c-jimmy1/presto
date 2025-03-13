package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.common.function.OperatorType;
import com.google.common.collect.ImmutableList;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.Optional;

import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
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
        implements Rule<FilterNode>
{
    private static final String DATE_FUNCTION = "date";
    private static final String YEAR_FUNCTION = "year";
    private static final String DATE_TRUNC_FUNCTION = "date_trunc";

    private final FunctionAndTypeManager functionAndTypeManager;
    private final StandardFunctionResolution functionResolution;

    public UnwrapDateFunctionInPredicate(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return typeOf(FilterNode.class);
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        RowExpression predicate = node.getPredicate();
        System.out.println("Original Predicate: " + predicate);

        Optional<RowExpression> rewritten = rewritePredicate(predicate);

        if (rewritten.isPresent()) {
            System.out.println("Rewritten Predicate: " + rewritten.get());
        } else {
            System.out.println("No transformation applied.");
        }

        return rewritten.map(rowExpression -> Result.ofPlanNode(
                new FilterNode(
                        node.getSourceLocation(),
                        node.getId(),
                        node.getSource(),
                        rowExpression))).orElseGet(Result::empty);

    }

    private Optional<RowExpression> rewritePredicate(RowExpression expression)
    {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression call = (CallExpression) expression;
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

    private Optional<RowExpression> tryRewriteFunctionEqualsLiteral(RowExpression functionSide, RowExpression literalSide)
    {
        if (!(functionSide instanceof CallExpression) || !(literalSide instanceof ConstantExpression)) {
            return Optional.empty();
        }

        CallExpression function = (CallExpression) functionSide;
        ConstantExpression literal = (ConstantExpression) literalSide;

        // Get function name
        String functionName = function.getDisplayName();

        if (functionName.equalsIgnoreCase(DATE_FUNCTION)) {
            return rewriteDateFunction(function, literal);
        }

        return Optional.empty();
    }

    private Optional<RowExpression> rewriteDateFunction(CallExpression function, ConstantExpression literal)
    {
        // Ensure this is date(column) = DATE '...'
        if (function.getArguments().size() != 1 || !(literal.getType() instanceof DateType)) {
            return Optional.empty();
        }

        RowExpression column = function.getArguments().get(0);

        // **Fix: Unwrap CAST if present**
        if (column instanceof CallExpression) {
            CallExpression castExpression = (CallExpression) column;
            if (castExpression.getDisplayName().equalsIgnoreCase("CAST") && castExpression.getArguments().size() == 1) {
                column = castExpression.getArguments().get(0); // Extract the original column
            }
        }

        // Ensure column is a valid reference
        if (!(column instanceof VariableReferenceExpression || column instanceof InputReferenceExpression)) {
            return Optional.empty();
        }

        // Extract date from literal and convert it to LocalDateTime
        LocalDate date = (LocalDate) literal.getValue();
        LocalDateTime lowerBound = date.atStartOfDay(); // Convert DATE -> TIMESTAMP
        LocalDateTime upperBound = lowerBound.plusDays(1);

        // Convert literal to TIMESTAMP type for comparison
        ConstantExpression lowerTimestampLiteral = new ConstantExpression(lowerBound, TIMESTAMP);
        ConstantExpression upperTimestampLiteral = new ConstantExpression(upperBound, TIMESTAMP);

        return Optional.of(createTimestampRangePredicate(column, lowerTimestampLiteral, upperTimestampLiteral));
    }


    private RowExpression createTimestampRangePredicate(
            RowExpression column,
            ConstantExpression lowerLiteral,
            ConstantExpression upperLiteral)
    {
        // Create comparisons: column >= lowerBound AND column < upperBound
        CallExpression lowerBoundComparison = new CallExpression(
                column.getSourceLocation(),
                "GREATER_THAN_OR_EQUAL",
                functionResolution.comparisonFunction(OperatorType.GREATER_THAN_OR_EQUAL, column.getType(), lowerLiteral.getType()),
                BOOLEAN,
                ImmutableList.of(column, lowerLiteral));

        CallExpression upperBoundComparison = new CallExpression(
                column.getSourceLocation(),
                "LESS_THAN",
                functionResolution.comparisonFunction(OperatorType.LESS_THAN, column.getType(), upperLiteral.getType()),
                BOOLEAN,
                ImmutableList.of(column, upperLiteral));

        // Combine with AND
        return new SpecialFormExpression(
                column.getSourceLocation(),
                AND,
                BOOLEAN,
                ImmutableList.of(lowerBoundComparison, upperBoundComparison));
    }

}