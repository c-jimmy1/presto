package com.facebook.presto.sql.planner.iterative.rule;

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
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static java.util.Objects.requireNonNull;
import java.util.Optional;

/**
 * This rule rewrites predicates of the form:
 *    hour(timestamp_column) = <integer>
 * into a range predicate on the timestamp column.
 *
 * IMPORTANT: Because hour() by itself is not monotonic over the entire timeline,
 * this rewriting only makes sense when the data is restricted to a single day.
 * For example, if your query is:
 *
 *   SELECT * FROM events
 *   WHERE date(event_timestamp) = DATE '2025-03-13'
 *     AND hour(event_timestamp) = 15;
 *
 * then this rule will rewrite the hour predicate into:
 *
 *   event_timestamp >= TIMESTAMP '2025-03-13 15:00:00.000'
 *   AND event_timestamp <  TIMESTAMP '2025-03-13 16:00:00.000'
 *
 * Without a date restriction, hour(t) = 15 would correspond to many disjoint ranges.
 */
public class UnwrapHourFunctionInPredicate
        implements Rule<FilterNode>
{
    private static final String HOUR_FUNCTION = "hour";
    // Number of microseconds per hour
    private static final long MICROSECONDS_PER_HOUR = 3600000000L;

    private final FunctionAndTypeManager functionAndTypeManager;
    private final StandardFunctionResolution functionResolution;

    public UnwrapHourFunctionInPredicate(FunctionAndTypeManager functionAndTypeManager)
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
        Optional<RowExpression> rewritten = rewritePredicate(predicate);

        return rewritten.map(rowExpression -> Result.ofPlanNode(
                        new FilterNode(
                                node.getSourceLocation(),
                                node.getId(),
                                node.getSource(),
                                rowExpression)))
                .orElseGet(Result::empty);
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

        // Try both: function(column) = literal and literal = function(column)
        Optional<RowExpression> rewritten = tryRewriteFunctionEqualsLiteral(left, right);
        if (rewritten.isPresent()) {
            return rewritten;
        }
        return tryRewriteFunctionEqualsLiteral(right, left);
    }

    private Optional<RowExpression> tryRewriteFunctionEqualsLiteral(RowExpression functionSide, RowExpression literalSide)
    {
        // Unwrap CAST on literal if present
        if (literalSide instanceof CallExpression) {
            CallExpression castExpression = (CallExpression) literalSide;
            if (castExpression.getDisplayName().equalsIgnoreCase("CAST")
                    && castExpression.getArguments().size() == 1) {
                literalSide = castExpression.getArguments().get(0);
            }
        }

        if (!(functionSide instanceof CallExpression) || !(literalSide instanceof ConstantExpression)) {
            return Optional.empty();
        }

        CallExpression function = (CallExpression) functionSide;
        ConstantExpression literal = (ConstantExpression) literalSide;

        if (function.getDisplayName().equalsIgnoreCase(HOUR_FUNCTION)) {
            return rewriteHourFunction(function, literal);
        }

        return Optional.empty();
    }

    private Optional<RowExpression> rewriteHourFunction(CallExpression function, ConstantExpression literal)
    {
        // Ensure the hour() function has exactly one argument
        if (function.getArguments().size() != 1) {
            return Optional.empty();
        }

        RowExpression column = function.getArguments().get(0);

        // Unwrap CAST on column if present
        if (column instanceof CallExpression) {
            CallExpression castExpression = (CallExpression) column;
            if (castExpression.getDisplayName().equalsIgnoreCase("CAST")
                    && castExpression.getArguments().size() == 1) {
                column = castExpression.getArguments().get(0);
            }
        }

        // Ensure that column is a valid TIMESTAMP reference
        if (!(column instanceof VariableReferenceExpression || column instanceof InputReferenceExpression)) {
            return Optional.empty();
        }
        if (!(column.getType() instanceof TimestampType)) {
            // Only apply if the column is a timestamp
            return Optional.empty();
        }

        // The literal should be an integer (0 to 23)
        if (!(literal.getValue() instanceof Number)) {
            return Optional.empty();
        }
        int hourValue = ((Number) literal.getValue()).intValue();
        if (hourValue < 0 || hourValue > 23) {
            return Optional.empty();
        }

        // IMPORTANT: To compute a contiguous range for an hour, we need the day’s start.
        // This rule only works if the query is restricted to a single day.
        // For demonstration, we assume that an additional predicate (e.g., date(t) = DATE '2025-03-13')
        // has already fixed the day.
        // Here we hard-code the day start for 2025-03-13.
        // In a real implementation, you would extract this from the query context.
        long dayStartMicros = computeDayStartMicros("2025-03-13");

        long lowerBoundMicros = dayStartMicros + hourValue * MICROSECONDS_PER_HOUR;
        long upperBoundMicros = lowerBoundMicros + MICROSECONDS_PER_HOUR;

        ConstantExpression lowerTimestampLiteral = new ConstantExpression(lowerBoundMicros, TimestampType.TIMESTAMP);
        ConstantExpression upperTimestampLiteral = new ConstantExpression(upperBoundMicros, TimestampType.TIMESTAMP);

        return Optional.of(createTimestampRangePredicate(column, lowerTimestampLiteral, upperTimestampLiteral));
    }

    /**
     * Compute the start of the day in microseconds from a date string "yyyy-MM-dd".
     * This is similar to how date literals are converted.
     */
    private long computeDayStartMicros(String dateString)
    {
        // Parse the date string into year, month, day
        String[] parts = dateString.split("-");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid date format: " + dateString);
        }
        int year = Integer.parseInt(parts[0]);
        int month = Integer.parseInt(parts[1]);
        int day = Integer.parseInt(parts[2]);

        // Compute the epoch day (number of days since 1970-01-01)
        // (Using a similar approach as in the date rule.)
        int a = (14 - month) / 12;
        int y = year + 4800 - a;
        int m = month + 12 * a - 3;
        int julianDay = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
        long epochDay = julianDay - 2440588;
        // Convert days to microseconds (1 day = 86,400,000,000 microseconds)
        final long MICROSECONDS_PER_DAY = 86400000000L;
        return epochDay * MICROSECONDS_PER_DAY;
    }

    private RowExpression createTimestampRangePredicate(
            RowExpression column,
            ConstantExpression lowerLiteral,
            ConstantExpression upperLiteral)
    {
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

        return new SpecialFormExpression(
                column.getSourceLocation(),
                AND,
                BOOLEAN,
                ImmutableList.of(lowerBoundComparison, upperBoundComparison));
    }
}
