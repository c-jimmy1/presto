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


}
