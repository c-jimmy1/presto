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
 *    date(timestamp_column) = DATE 'YYYY-MM-DD'
 * into:
 *    timestamp_column >= TIMESTAMP 'YYYY-MM-DD 00:00:00.000'
 * AND timestamp_column < TIMESTAMP 'YYYY-MM-DD+1 00:00:00.000'
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

        // try rewriting date(...) = date-literal
        Optional<RowExpression> rewritten = tryRewriteFunctionEqualsLiteral(left, right);
        if (!rewritten.isPresent()) {
            // or date-literal = date(...)
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
        CallExpression dateCall = (CallExpression) functionSide;

        // must be date(...)
        if (!DATE_FUNCTION.equalsIgnoreCase(dateCall.getDisplayName())) {
            return Optional.empty();
        }
        if (dateCall.getArguments().size() != 1) {
            return Optional.empty();
        }

        // the argument to date() must be a TIMESTAMP
        RowExpression timestampExpr = unwrapCasts(dateCall.getArguments().get(0));
        if (!(timestampExpr.getType() instanceof TimestampType)) {
            return Optional.empty();
        }

        // try to interpret the literal side as a DATE expression
        RowExpression DateLiteral = unwrapDateLiteralIfConstant(literalSide);
        if (DateLiteral == null) {
            return Optional.empty();
        }
        if (!(DateLiteral.getType() instanceof DateType)) {
            return Optional.empty();
        }

        if (!(DateLiteral instanceof ConstantExpression)) {
            // not fully folded to a constant? bail for simplicity
            return Optional.empty();
        }
        ConstantExpression dateConstant = (ConstantExpression) DateLiteral;

        // rewrite date(timestamp_col) = DATE 'xxx' into the TS range
        return rewriteDateFunction(timestampExpr, dateConstant);
    }

    /**
     * If the literal side is something like CAST('YYYY-MM-DD' AS date),
     * parse it into a ConstantExpression(date).
     * If it's already a ConstantExpression(date), return it directly.
     * Otherwise, return null.
     */
    private RowExpression unwrapDateLiteralIfConstant(RowExpression expression)
    {
        // If it's already a date constant, great
        if (expression instanceof ConstantExpression) {
            ConstantExpression constant = (ConstantExpression) expression;
            if (constant.getType() instanceof DateType) {
                return constant;
            }
            return null; // not a DATE
        }

        // If it's a cast call -> type DATE
        if (expression instanceof CallExpression) {
            CallExpression castCall = (CallExpression) expression;
            if (!functionResolution.isCastFunction(castCall.getFunctionHandle())) {
                return null;
            }
            if (!(castCall.getType() instanceof DateType)) {
                return null;
            }
            if (castCall.getArguments().size() != 1) {
                return null;
            }

            RowExpression child = castCall.getArguments().get(0);
            // If the child is a constant varchar or constant slice, parse it
            if (child instanceof ConstantExpression) {
                ConstantExpression ce = (ConstantExpression) child;
                // Often, a string literal in Presto is stored as a Slice in Java
                // (io.airlift.slice.Slice). Let's parse it to a date.
                if (ce.getValue() instanceof Slice) {
                    Slice slice = (Slice) ce.getValue();
                    String dateString = slice.toStringUtf8();
                    try {
                        LocalDate localDate = LocalDate.parse(dateString);
                        long epochDay = localDate.toEpochDay();
                        // Build a new DATE constant
                        return new ConstantExpression(
                                castCall.getSourceLocation(),
                                epochDay,
                                DateType.DATE);
                    }
                    catch (DateTimeParseException e) {
                        // not a valid date?
                        return null;
                    }
                }
            }
        }

        // not recognized
        return null;
    }

    /**
     * Rewrites:
     *   date(timestamp) = constantDate
     * into:
     *   timestamp >= (constantDate cast as timestamp)
     *   AND timestamp <  (constantDate+1 day cast as timestamp)
     */
    private Optional<RowExpression> rewriteDateFunction(RowExpression timestampExpr, ConstantExpression dateLiteral)
    {
        long epochDays = ((Number) dateLiteral.getValue()).longValue();

        // build two date constants
        ConstantExpression lowerDate = new ConstantExpression(dateLiteral.getSourceLocation(), epochDays, DateType.DATE);
        ConstantExpression upperDate = new ConstantExpression(dateLiteral.getSourceLocation(), epochDays + 1, DateType.DATE);

        // cast them to timestamp
        CallExpression lowerTs = buildDateToTimestampCast(lowerDate);
        CallExpression upperTs = buildDateToTimestampCast(upperDate);

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

    private CallExpression buildDateToTimestampCast(ConstantExpression dateConstant)
    {
        FunctionHandle castHandle = functionAndTypeManager.lookupCast(
                CastType.CAST,
                dateConstant.getType(),
                TimestampType.TIMESTAMP);

        return new CallExpression(
                dateConstant.getSourceLocation(),
                OperatorType.CAST.name(),
                castHandle,
                TimestampType.TIMESTAMP,
                ImmutableList.of(dateConstant));
    }
}
