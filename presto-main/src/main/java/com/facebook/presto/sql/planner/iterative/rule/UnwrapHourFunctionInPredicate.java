/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static java.util.Objects.requireNonNull;

/**
 * This rule rewrites predicates of the form:
 *    hour(timestamp_column) = INTEGER
 * into:
 *    timestamp_column >= TIMESTAMP 'YYYY-MM-DD HH:00:00.000'
 * AND timestamp_column < TIMESTAMP 'YYYY-MM-DD HH+1:00:00.000'
 */
public class UnwrapHourFunctionInPredicate
        implements Rule<FilterNode>
{
    private static final String HOUR_FUNCTION = "hour";

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

        if (!(predicate instanceof CallExpression)) {
            return Result.empty();
        }
        CallExpression call = (CallExpression) predicate;

        // check it's an equality call
        if (!functionResolution.isEqualsFunction(call.getFunctionHandle())) {
            return Result.empty();
        }

        // unwrap purely redundant casts, e.g. CAST(x AS timestamp) where x is already timestamp
        RowExpression left = unwrapCasts(call.getArguments().get(0));
        RowExpression right = unwrapCasts(call.getArguments().get(1));

        // try rewriting hour(...) = integer-literal
        Optional<RowExpression> rewritten = tryRewriteFunctionEqualsLiteral(left, right);
        if (!rewritten.isPresent()) {
            // or integer-literal = hour(...)
            rewritten = tryRewriteFunctionEqualsLiteral(right, left);
        }

        return rewritten.map(rowExpression -> Result.ofPlanNode(
                new FilterNode(
                        node.getSourceLocation(),
                        node.getId(),
                        node.getSource(),
                        rowExpression))).orElseGet(Result::empty);
    }

    private Optional<RowExpression> tryRewriteFunctionEqualsLiteral(RowExpression functionSide, RowExpression literalSide)
    {
        if (!(functionSide instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression hourCall = (CallExpression) functionSide;

        // must be hour(...)
        if (!HOUR_FUNCTION.equalsIgnoreCase(hourCall.getDisplayName())) {
            return Optional.empty();
        }
        if (hourCall.getArguments().size() != 1) {
            return Optional.empty();
        }

        // the argument to hour() must be a TIMESTAMP
        RowExpression timestampExpr = unwrapCasts(hourCall.getArguments().get(0));
        if (!(timestampExpr.getType() instanceof TimestampType)) {
            return Optional.empty();
        }

        // try to interpret the literal side as an INTEGER
        RowExpression integerLiteral = unwrapIntegerLiteralIfConstant(literalSide);
        if (integerLiteral == null) {
            return Optional.empty();
        }

        if (!(integerLiteral instanceof ConstantExpression)) {
            // not a constant? bail for simplicity
            return Optional.empty();
        }
        ConstantExpression integerConstant = (ConstantExpression) integerLiteral;

        // rewrite hour(timestamp_col) = INTEGER into the timestamp range for the hour
        return rewriteHourFunction(timestampExpr, integerConstant);
    }

    /**
     * If the literal side is something like CAST('HH' AS integer),
     * parse it into a ConstantExpression(integer).
     * If it's already a ConstantExpression(integer), return it directly.
     * Otherwise, return null.
     */
    private RowExpression unwrapIntegerLiteralIfConstant(RowExpression expression)
    {
        // If it's already an integer constant, great
        if (expression instanceof ConstantExpression) {
            ConstantExpression constant = (ConstantExpression) expression;
            if (constant.getType() instanceof TimestampType) {
                return constant;
            }
            return null; // not an integer
        }

        // If it's a cast call -> type INTEGER
        if (expression instanceof CallExpression) {
            CallExpression castCall = (CallExpression) expression;
            if (!functionResolution.isCastFunction(castCall.getFunctionHandle())) {
                return null;
            }
            if (!(castCall.getType() instanceof TimestampType)) {
                return null;
            }
            if (castCall.getArguments().size() != 1) {
                return null;
            }

            RowExpression child = castCall.getArguments().get(0);
            if (child instanceof ConstantExpression) {
                ConstantExpression ce = (ConstantExpression) child;
                if (ce.getValue() instanceof Slice) {
                    Slice slice = (Slice) ce.getValue();
                    try {
                        return new ConstantExpression(ce.getSourceLocation(), Integer.parseInt(slice.toStringUtf8()), IntegerType.INTEGER);
                    } catch (NumberFormatException e) {
                        // not a valid integer?
                        return null;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Rewrites:
     *   hour(timestamp_col) = constantInteger
     * into:
     *   timestamp >= (constantInteger:00:00:00.000)
     * AND timestamp <  (constantInteger+1:00:00.000)
     */
    private Optional<RowExpression> rewriteHourFunction(RowExpression timestampExpr, ConstantExpression hourLiteral)
    {
        int hour = (Integer) hourLiteral.getValue();

        // build two timestamp constants: hourStart and hourEnd
        ConstantExpression lowerTimestamp = new ConstantExpression(hourLiteral.getSourceLocation(), hour, TimestampType.TIMESTAMP);
        ConstantExpression upperTimestamp = new ConstantExpression(hourLiteral.getSourceLocation(), hour + 1, TimestampType.TIMESTAMP);

        // cast them to timestamp
        CallExpression lowerTs = buildHourToTimestampCast(lowerTimestamp);
        CallExpression upperTs = buildHourToTimestampCast(upperTimestamp);

        // timestamp >= lowerTs
        RowExpression lowerBound = new CallExpression(
                timestampExpr.getSourceLocation(),
                OperatorType.GREATER_THAN_OR_EQUAL.name(),
                functionResolution.comparisonFunction(
                        OperatorType.GREATER_THAN_OR_EQUAL,
                        timestampExpr.getType(),
                        lowerTs.getType()),
                BOOLEAN,
                ImmutableList.of(timestampExpr, lowerTs));

        // timestamp < upperTs
        RowExpression upperBound = new CallExpression(
                timestampExpr.getSourceLocation(),
                OperatorType.LESS_THAN.name(),
                functionResolution.comparisonFunction(
                        OperatorType.LESS_THAN,
                        timestampExpr.getType(),
                        upperTs.getType()),
                BOOLEAN,
                ImmutableList.of(timestampExpr, upperTs));

        // combine with AND
        SpecialFormExpression finalPredicate = new SpecialFormExpression(
                timestampExpr.getSourceLocation(),
                AND,
                BOOLEAN,
                ImmutableList.of(lowerBound, upperBound));

        return Optional.of(finalPredicate);
    }

    /**
     * This unwrapping only removes redundant casts, i.e. cast(x as T) where x is already T.
     */
    private RowExpression unwrapCasts(RowExpression childExpression)
    {
        while (childExpression instanceof CallExpression) {
            CallExpression call = (CallExpression) childExpression;
            if (functionResolution.isCastFunction(call.getFunctionHandle())
                    && call.getArguments().size() == 1) {
                // only remove if the cast input type == cast output type
                RowExpression grandchild = call.getArguments().get(0);
                if (call.getType().equals(grandchild.getType())) {
                    childExpression = grandchild;
                    continue;
                }
            }
            break;
        }
        return childExpression;
    }

    private CallExpression buildHourToTimestampCast(ConstantExpression hourConstant)
    {
        FunctionHandle castHandle = functionAndTypeManager.lookupCast(
                CastType.CAST,
                hourConstant.getType(),
                TimestampType.TIMESTAMP);

        return new CallExpression(
                hourConstant.getSourceLocation(),
                OperatorType.CAST.name(),
                castHandle,
                TimestampType.TIMESTAMP,
                ImmutableList.of(hourConstant));
    }
}
