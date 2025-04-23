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

import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static java.util.Objects.requireNonNull;

public class UnwrapYearFunctionInPredicate
        implements Rule<FilterNode>
{
    private static final String YEAR_FUNCTION = "year";

    private final FunctionAndTypeManager functionAndTypeManager;
    private final StandardFunctionResolution functionResolution;

    public UnwrapYearFunctionInPredicate(FunctionAndTypeManager functionAndTypeManager)
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

        // Check it's an equality call for the year function
        if (!functionResolution.isEqualsFunction(call.getFunctionHandle())) {
            return Result.empty();
        }

        // Unwrap redundant casts for the left and right side of the predicate
        RowExpression left = unwrapCasts(call.getArguments().get(0));
        RowExpression right = unwrapCasts(call.getArguments().get(1));

        // Try rewriting year(...) = integer-literal
        Optional<RowExpression> rewritten = tryRewriteYearFunctionEqualsLiteral(left, right);
        if (!rewritten.isPresent()) {
            // or integer-literal = year(...)
            rewritten = tryRewriteYearFunctionEqualsLiteral(right, left);
        }

        return rewritten.map(rowExpression -> Result.ofPlanNode(
                new FilterNode(
                        node.getSourceLocation(),
                        node.getId(),
                        node.getSource(),
                        rowExpression))).orElseGet(Result::empty);
    }

    private Optional<RowExpression> tryRewriteYearFunctionEqualsLiteral(RowExpression functionSide, RowExpression literalSide)
    {
        if (!(functionSide instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression yearCall = (CallExpression) functionSide;

        // Must be year(...)
        if (!YEAR_FUNCTION.equalsIgnoreCase(yearCall.getDisplayName())) {
            return Optional.empty();
        }
        if (yearCall.getArguments().size() != 1) {
            return Optional.empty();
        }

        // The argument to year() must be a TIMESTAMP
        RowExpression timestampExpr = unwrapCasts(yearCall.getArguments().get(0));
        if (!(timestampExpr.getType() instanceof TimestampType)) {
            return Optional.empty();
        }

        // Try to interpret the literal side as an integer
        RowExpression integerLiteral = unwrapIntegerLiteralIfConstant(literalSide);
        if (integerLiteral == null) {
            return Optional.empty();
        }

        if (!(integerLiteral instanceof ConstantExpression)) {
            return Optional.empty();
        }
        ConstantExpression integerConstant = (ConstantExpression) integerLiteral;

        // Rewrite year(timestamp_col) = INTEGER into a range check
        return rewriteYearFunction(timestampExpr, integerConstant);
    }

    private RowExpression unwrapIntegerLiteralIfConstant(RowExpression expression)
    {
        if (expression instanceof ConstantExpression) {
            ConstantExpression constant = (ConstantExpression) expression;
            if (constant.getType() instanceof IntegerType) {
                return constant;
            }
            return null; // Not an integer
        }
        return null; // Not a constant
    }

    private Optional<RowExpression> rewriteYearFunction(RowExpression timestampExpr, ConstantExpression yearLiteral)
    {
        int year = (Integer) yearLiteral.getValue();

        // Build two timestamp constants: yearStart and yearEnd
        ConstantExpression lowerTimestamp = new ConstantExpression(yearLiteral.getSourceLocation(), year, TimestampType.TIMESTAMP);
        ConstantExpression upperTimestamp = new ConstantExpression(yearLiteral.getSourceLocation(), year + 1, TimestampType.TIMESTAMP);

        // Cast them to timestamp
        CallExpression lowerTs = buildYearToTimestampCast(lowerTimestamp);
        CallExpression upperTs = buildYearToTimestampCast(upperTimestamp);

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

        // Combine with AND
        SpecialFormExpression finalPredicate = new SpecialFormExpression(
                timestampExpr.getSourceLocation(),
                AND,
                BOOLEAN,
                ImmutableList.of(lowerBound, upperBound));

        return Optional.of(finalPredicate);
    }

    private CallExpression buildYearToTimestampCast(ConstantExpression yearConstant)
    {
        FunctionHandle castHandle = functionAndTypeManager.lookupCast(
                CastType.CAST,
                yearConstant.getType(),
                TimestampType.TIMESTAMP);

        return new CallExpression(
                yearConstant.getSourceLocation(),
                OperatorType.CAST.name(),
                castHandle,
                TimestampType.TIMESTAMP,
                ImmutableList.of(yearConstant));
    }



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
}
