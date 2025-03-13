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
import io.airlift.slice.Slice;

import java.util.Optional;

import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
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
        System.out.println("functionSide: " + functionSide + " (" + functionSide.getClass().getName() + ")");
        System.out.println("literalSide: " + literalSide + " (" + literalSide.getClass().getName() + ")");

        // Unwrap CAST if present on the literal side
        if (literalSide instanceof CallExpression) {
            CallExpression castExpression = (CallExpression) literalSide;
            if (castExpression.getDisplayName().equalsIgnoreCase("CAST") && castExpression.getArguments().size() == 1) {
                literalSide = castExpression.getArguments().get(0); // Extract the inner literal
            }
        }

        if (!(functionSide instanceof CallExpression) || !(literalSide instanceof ConstantExpression)) {
            return Optional.empty();
        }

        CallExpression function = (CallExpression) functionSide;
        ConstantExpression literal = (ConstantExpression) literalSide;

        String functionName = function.getDisplayName();

        if (functionName.equalsIgnoreCase(DATE_FUNCTION)) {
            return rewriteDateFunction(function, literal);
        }

        return Optional.empty();
    }

    private Optional<RowExpression> rewriteDateFunction(CallExpression function, ConstantExpression literal)
    {
        System.out.println("Function: " + function + " (" + function.getClass().getName() + ")");
        System.out.println("Function Arguments: " + function.getArguments());
        System.out.println("Literal: " + literal + " (" + literal.getClass().getName() + ")");
        System.out.println("Literal Type: " + literal.getType());

        // Ensure this is date(column) = DATE '...'
        if (function.getArguments().size() != 1) {
            System.out.println("Function has unexpected number of arguments.");
            return Optional.empty();
        }

        RowExpression column = function.getArguments().get(0);

        // Unwrap CAST if present on the column
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

        // Handle case where literal is VARCHAR (represented internally as io.airlift.slice.Slice) instead of DATE
        if (literal.getType().getJavaType().equals(Slice.class)) {
            try {
                Slice slice = (Slice) literal.getValue();
                String dateString = slice.toStringUtf8();
                long epochDay = parseEpochDay(dateString);
                literal = new ConstantExpression(epochDay, DateType.DATE);
                System.out.println("Converted VARCHAR literal to DateType using epochDay: " + epochDay);
            } catch (Exception e) {
                System.out.println("Failed to parse VARCHAR as DateType: " + e.getMessage());
                return Optional.empty();
            }
        }

        // Ensure that literal is now a DateType
        if (!(literal.getType() instanceof DateType)) {
            System.out.println("Literal type is still not DateType, exiting.");
            return Optional.empty();
        }

        // Extract the epoch day from the literal (as a long)
        Object value = literal.getValue();
        long dateDays;
        if (value instanceof Number) {
            dateDays = ((Number) value).longValue();
        }
        else {
            System.out.println("Unexpected literal value type: " + value.getClass());
            return Optional.empty();
        }

        // Compute the timestamp range in microseconds.
        // Presto TIMESTAMP literals are represented as microseconds since the Unix epoch.
        final long MICROSECONDS_PER_DAY = 86400000000L;
        long lowerBoundMicros = dateDays * MICROSECONDS_PER_DAY;
        long upperBoundMicros = (dateDays + 1) * MICROSECONDS_PER_DAY;

        // Create timestamp literals for comparison
        ConstantExpression lowerTimestampLiteral = new ConstantExpression(lowerBoundMicros, TimestampType.TIMESTAMP);
        ConstantExpression upperTimestampLiteral = new ConstantExpression(upperBoundMicros, TimestampType.TIMESTAMP);

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

        // Combine the two comparisons with an AND
        return new SpecialFormExpression(
                column.getSourceLocation(),
                AND,
                BOOLEAN,
                ImmutableList.of(lowerBoundComparison, upperBoundComparison));
    }

    /**
     * Parses a date string in the format "yyyy-MM-dd" and returns the epoch day.
     * The epoch day is the number of days since 1970-01-01.
     */
    private long parseEpochDay(String dateString)
    {
        String[] parts = dateString.split("-");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid date format: " + dateString);
        }
        int year = Integer.parseInt(parts[0]);
        int month = Integer.parseInt(parts[1]);
        int day = Integer.parseInt(parts[2]);

        // Convert the date to Julian Day using the algorithm:
        // a = (14 - month) / 12
        // y = year + 4800 - a
        // m = month + 12 * a - 3
        // julianDay = day + (153*m + 2)/5 + 365*y + y/4 - y/100 + y/400 - 32045
        int a = (14 - month) / 12;
        int y = year + 4800 - a;
        int m = month + 12 * a - 3;
        int julianDay = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
        // Unix epoch (1970-01-01) corresponds to Julian Day 2440588
        return julianDay - 2440588;
    }
}
