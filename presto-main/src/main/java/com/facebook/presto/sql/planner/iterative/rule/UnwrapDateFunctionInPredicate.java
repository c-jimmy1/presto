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
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;

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
        // This is typically needed to check if functionHandle == EQUALS, or build comparisonFunction, etc.
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
            return Result.ofPlanNode(
                    new FilterNode(
                            node.getSourceLocation(),
                            node.getId(),
                            node.getSource(),
                            rewritten.get()));
        }
        else {
            System.out.println("No transformation applied.");
            return Result.empty();
        }
    }

    private Optional<RowExpression> rewritePredicate(RowExpression expression)
    {
        // We're only rewriting "date(...) = literal"
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

        // Try function(...)=literal and literal=function(...)
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

        // If the "literalSide" is itself a CAST expression, unwrap it
        if (literalSide instanceof CallExpression) {
            CallExpression castExpression = (CallExpression) literalSide;
            if (isCast(castExpression) && castExpression.getArguments().size() == 1) {
                literalSide = castExpression.getArguments().get(0);
            }
        }

        // Must have function(...) on one side and a constant on the other
        if (!(functionSide instanceof CallExpression) || !(literalSide instanceof ConstantExpression)) {
            return Optional.empty();
        }

        CallExpression functionCall = (CallExpression) functionSide;
        ConstantExpression literal = (ConstantExpression) literalSide;

        // Check if the function call is "date(...)"
        if (!DATE_FUNCTION.equalsIgnoreCase(functionCall.getDisplayName())) {
            return Optional.empty();
        }

        return rewriteDateFunction(functionCall, literal);
    }

    /**
     * Rewrites date(column) = DATE-literal into:
     *   column >= CAST(DATE-literal AS TIMESTAMP)
     *   AND column < CAST(DATE-literal+1 AS TIMESTAMP)
     */
    private Optional<RowExpression> rewriteDateFunction(CallExpression function, ConstantExpression literal)
    {
        // Must have exactly 1 argument: date(timestamp_column)
        if (function.getArguments().size() != 1) {
            return Optional.empty();
        }

        RowExpression column = function.getArguments().get(0);

        // If the column is a CAST expression, unwrap it. (Optional)
        if (column instanceof CallExpression) {
            CallExpression castExpression = (CallExpression) column;
            if (isCast(castExpression)) {
                column = castExpression.getArguments().get(0);
            }
        }

        // Ensure we have something like variable or input reference
        if (!(column instanceof VariableReferenceExpression || column instanceof InputReferenceExpression)) {
            return Optional.empty();
        }

        // The literal must be a DATE typed constant
        if (!(literal.getType() instanceof DateType)) {
            return Optional.empty();
        }

        // The literal's value is the epoch day as a long
        long dateDays = ((Number) literal.getValue()).longValue();

        // Create two new DATE constants: one for the day, one for day+1
        ConstantExpression lowerDateConstant = new ConstantExpression(dateDays, DateType.DATE);
        ConstantExpression upperDateConstant = new ConstantExpression(dateDays + 1, DateType.DATE);

        // CAST both date constants to TIMESTAMP
        CallExpression lowerTimestamp = buildDateToTimestampCast(lowerDateConstant);
        CallExpression upperTimestamp = buildDateToTimestampCast(upperDateConstant);

        // column >= lowerTimestamp
        RowExpression lowerBoundComparison = new CallExpression(
                column.getSourceLocation(),
                OperatorType.GREATER_THAN_OR_EQUAL.name(),
                functionResolution.comparisonFunction(OperatorType.GREATER_THAN_OR_EQUAL, column.getType(), lowerTimestamp.getType()),
                BOOLEAN,
                ImmutableList.of(column, lowerTimestamp));

        // column < upperTimestamp
        RowExpression upperBoundComparison = new CallExpression(
                column.getSourceLocation(),
                OperatorType.LESS_THAN.name(),
                functionResolution.comparisonFunction(OperatorType.LESS_THAN, column.getType(), upperTimestamp.getType()),
                BOOLEAN,
                ImmutableList.of(column, upperTimestamp));

        // Combine them: AND(...)
        RowExpression finalPredicate = new SpecialFormExpression(
                column.getSourceLocation(),
                AND,
                BOOLEAN,
                ImmutableList.of(lowerBoundComparison, upperBoundComparison));

        return Optional.of(finalPredicate);
    }

    private boolean isCast(CallExpression expression)
    {
        // Checking if the display name is CAST and it has exactly 1 argument is enough in many Presto/Trino versions
        return expression.getDisplayName().equalsIgnoreCase(OperatorType.CAST.getFunctionName().toString())
                && expression.getArguments().size() == 1;
    }

    /**
     * Builds a CAST(DATE -> TIMESTAMP) call expression using functionAndTypeManager.lookupCast(...).
     */
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
