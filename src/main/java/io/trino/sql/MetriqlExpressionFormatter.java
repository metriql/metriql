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
package io.trino.sql;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.hubspot.jinjava.Jinjava;
import com.metriql.service.model.Model;
import com.metriql.warehouse.spi.function.RFunction;
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext;
import io.trino.sql.tree.*;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.MetriqlSqlFormatter.formatSql;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class MetriqlExpressionFormatter {
    private static final ThreadLocal<DecimalFormat> doubleFormatter = ThreadLocal.withInitial(
            () -> new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));
    private static final Jinjava renderer = new Jinjava();

    private MetriqlExpressionFormatter() {
    }

    public static String formatExpression(Expression expression, SqlToSegmentation reWriter, IQueryGeneratorContext context, List<Model> models) {
        return new Formatter(reWriter, context, models).process(expression, null);
    }

    protected static String formatIdentifier(String rawValue, IQueryGeneratorContext context) {
        return context.getWarehouseBridge().quoteIdentifier(rawValue);
    }

    public static class Formatter extends AstVisitor<String, Void> {
        protected final IQueryGeneratorContext queryContext;
        private final List<Model> models;
        private final SqlToSegmentation reWriter;

        public Formatter(SqlToSegmentation reWriter, IQueryGeneratorContext queryContext, List<Model> models) {
            this.reWriter = reWriter;
            this.queryContext = queryContext;
            this.models = models;
        }

        @Override
        protected String visitNode(Node node, Void context) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitRow(Row node, Void context) {
            return "ROW (" + Joiner.on(", ").join(node.getItems().stream()
                    .map((child) -> process(child, context))
                    .collect(toList())) + ")";
        }

        @Override
        protected String visitExpression(Expression node, Void context) {
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitAtTimeZone(AtTimeZone node, Void context) {
            return new StringBuilder()
                    .append(process(node.getValue(), context))
                    .append(" AT TIME ZONE ")
                    .append(process(node.getTimeZone(), context)).toString();
        }

        @Override
        protected String visitCurrentCatalog(CurrentCatalog node, Void context) {
            return "CURRENT_CATALOG";
        }

        @Override
        protected String visitCurrentSchema(CurrentSchema node, Void context) {
            return "CURRENT_SCHEMA";
        }

        @Override
        protected String visitCurrentUser(CurrentUser node, Void context) {
            return "CURRENT_USER";
        }

        @Override
        protected String visitCurrentPath(CurrentPath node, Void context) {
            return "CURRENT_PATH";
        }

        @Override
        protected String visitFormat(Format node, Void context) {
            return "format(" + joinExpressions(node.getArguments()) + ")";
        }

        @Override
        protected String visitCurrentTime(CurrentTime node, Void context) {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getFunction().getName());

            if (node.getPrecision() != null) {
                builder.append('(')
                        .append(node.getPrecision())
                        .append(')');
            }

            return builder.toString();
        }

        @Override
        protected String visitExtract(Extract node, Void context) {
            return "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), context) + ")";
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context) {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context) {
            return formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitCharLiteral(CharLiteral node, Void context) {
            return "CHAR " + formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitBinaryLiteral(BinaryLiteral node, Void context) {
            return "X'" + node.toHexString() + "'";
        }

        @Override
        protected String visitParameter(Parameter node, Void context) {
            return "?";
        }

        @Override
        protected String visitAllRows(AllRows node, Void context) {
            return "ALL";
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context) {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context) {
            return doubleFormatter.get().format(node.getValue());
        }

        @Override
        protected String visitDecimalLiteral(DecimalLiteral node, Void context) {
            // TODO return node value without "DECIMAL '..'" when FeaturesConfig#parseDecimalLiteralsAsDouble switch is removed
            return "DECIMAL '" + node.getValue() + "'";
        }

        @Override
        protected String visitGenericLiteral(GenericLiteral node, Void context) {
            return node.getType() + " " + formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitTimeLiteral(TimeLiteral node, Void context) {
            return "TIME '" + node.getValue() + "'";
        }

        @Override
        protected String visitTimestampLiteral(TimestampLiteral node, Void context) {
            return "TIMESTAMP '" + node.getValue() + "'";
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Void context) {
            return "null";
        }

        @Override
        protected String visitIntervalLiteral(IntervalLiteral node, Void context) {
            String sign = (node.getSign() == IntervalLiteral.Sign.NEGATIVE) ? "- " : "";
            StringBuilder builder = new StringBuilder()
                    .append("INTERVAL ")
                    .append(sign)
                    .append(" '").append(node.getValue()).append("' ")
                    .append(node.getStartField());

            if (node.getEndField().isPresent()) {
                builder.append(" TO ").append(node.getEndField().get());
            }
            return builder.toString();
        }

        @Override
        protected String visitSubqueryExpression(SubqueryExpression node, Void context) {
            return "(" + formatSql(node.getQuery(), reWriter, queryContext, models) + ")";
        }

        @Override
        protected String visitExists(ExistsPredicate node, Void context) {
            return "(EXISTS " + formatSql(node.getSubquery(), reWriter, queryContext, models) + ")";
        }

        @Override
        protected String visitIdentifier(Identifier node, Void context) {
            if (!node.isDelimited()) {
                return node.getValue();
            } else {
                return formatIdentifier(node.getValue(), queryContext);
            }
        }

        @Override
        protected String visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, Void context) {
            return formatExpression(node.getName(), reWriter, queryContext, models);
        }

        @Override
        protected String visitSymbolReference(SymbolReference node, Void context) {
            return formatIdentifier(node.getName(), queryContext);
        }

        @Override
        protected String visitDereferenceExpression(DereferenceExpression node, Void context) {
            String baseString = process(node.getBase(), context);
            return baseString + "." + process(node.getField());
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context) {
            StringBuilder builder = new StringBuilder();

            if (node.getProcessingMode().isPresent()) {
                throw new UnsupportedOperationException("Processing mode is not supported");
            }
            if (node.isDistinct()) {
                throw new UnsupportedOperationException("DISTINCT in a function is not supported");
            }
            if (node.getOrderBy().isPresent()) {
                throw new UnsupportedOperationException("Order by in function is not supported");
            }

            if (node.getFilter().isPresent()) {
                throw new UnsupportedOperationException("Filter in function is not supported");
            }

            if (node.getWindow().isPresent()) {
                throw new UnsupportedOperationException("WINDOW function is not supported");
            }

            Map<RFunction, String> functions = queryContext.getDatasource().getWarehouse().getBridge().getFunctions();
            String name = node.getName().getSuffix().toUpperCase();

            String functionTemplate;
            try {
                functionTemplate = functions.get(RFunction.valueOf(name));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("Function %s not supported", node.getName().getSuffix()));
            }

            if (functionTemplate == null) {
                String dbName = queryContext.getDatasource().getWarehouse().getNames().component1();
                throw new IllegalArgumentException(String.format("Function %s not supported in %s", node.getName().getSuffix(), dbName));
            }
            HashMap<String, Object> templateParams = new HashMap<>();

            List<String> arguments = node.getArguments().stream().map((e) -> process(e, null))
                    .collect(Collectors.toList());
            templateParams.put("value", arguments);

            String renderedFunction = renderer.render(functionTemplate, templateParams);
            builder.append(renderedFunction);

            node.getNullTreatment().ifPresent(nullTreatment -> {
                switch (nullTreatment) {
                    case IGNORE:
                        builder.append(" IGNORE NULLS");
                        break;
                    case RESPECT:
                        builder.append(" RESPECT NULLS");
                        break;
                }
            });

            return builder.toString();
        }

        @Override
        protected String visitLambdaExpression(LambdaExpression node, Void context) {
            StringBuilder builder = new StringBuilder();

            builder.append('(');
            Joiner.on(", ").appendTo(builder, node.getArguments());
            builder.append(") -> ");
            builder.append(process(node.getBody(), context));
            return builder.toString();
        }

        @Override
        protected String visitBindExpression(BindExpression node, Void context) {
            StringBuilder builder = new StringBuilder();

            builder.append("\"$INTERNAL$BIND\"(");
            for (Expression value : node.getValues()) {
                builder.append(process(value, context))
                        .append(", ");
            }
            builder.append(process(node.getFunction(), context))
                    .append(")");
            return builder.toString();
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
            return formatBinaryExpression(node.getOperator().toString(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitNotExpression(NotExpression node, Void context) {
            return "(NOT " + process(node.getValue(), context) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context) {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " IS NULL)";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " IS NOT NULL)";
        }

        @Override
        protected String visitNullIfExpression(NullIfExpression node, Void context) {
            return "NULLIF(" + process(node.getFirst(), context) + ", " + process(node.getSecond(), context) + ')';
        }

        @Override
        protected String visitIfExpression(IfExpression node, Void context) {
            StringBuilder builder = new StringBuilder();
            builder.append("IF(")
                    .append(process(node.getCondition(), context))
                    .append(", ")
                    .append(process(node.getTrueValue(), context));
            if (node.getFalseValue().isPresent()) {
                builder.append(", ")
                        .append(process(node.getFalseValue().get(), context));
            }
            builder.append(")");
            return builder.toString();
        }

        @Override
        protected String visitTryExpression(TryExpression node, Void context) {
            return "TRY(" + process(node.getInnerExpression(), context) + ")";
        }

        @Override
        protected String visitCoalesceExpression(CoalesceExpression node, Void context) {
            return "COALESCE(" + joinExpressions(node.getOperands()) + ")";
        }

        @Override
        protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
            String value = process(node.getValue(), context);

            switch (node.getSign()) {
                case MINUS:
                    // Unary is ambiguous with respect to negative numbers. "-1" parses as a number, but "-(1)" parses as "unaryMinus(number)"
                    // The parentheses are needed to ensure the parsing roundtrips properly.
                    return "-(" + value + ")";
                case PLUS:
                    return "+" + value;
            }
            throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
        }

        @Override
        protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Void context) {
            StringBuilder builder = new StringBuilder();

            builder.append('(')
                    .append(process(node.getValue(), context))
                    .append(" LIKE ")
                    .append(process(node.getPattern(), context));

            node.getEscape().ifPresent(escape -> builder.append(" ESCAPE ")
                    .append(process(escape, context)));

            builder.append(')');

            return builder.toString();
        }

        @Override
        protected String visitAllColumns(AllColumns node, Void context) {
            StringBuilder builder = new StringBuilder();
            if (node.getTarget().isPresent()) {
                builder.append(process(node.getTarget().get(), context));
                builder.append(".*");
            } else {
                builder.append("*");
            }

            if (!node.getAliases().isEmpty()) {
                builder.append(" AS (");
                Joiner.on(", ").appendTo(builder, node.getAliases().stream()
                        .map(alias -> process(alias, context))
                        .collect(toList()));
                builder.append(")");
            }

            return builder.toString();
        }

        @Override
        public String visitCast(Cast node, Void context) {
            return (node.isSafe() ? "TRY_CAST" : "CAST") +
                    "(" + process(node.getExpression(), context) + " AS " + process(node.getType(), context) + ")";
        }

        @Override
        protected String visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
            ImmutableList.Builder<String> parts = ImmutableList.builder();
            parts.add("CASE");
            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, context));
            }

            node.getDefaultValue()
                    .ifPresent((value) -> parts.add("ELSE").add(process(value, context)));

            parts.add("END");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        @Override
        protected String visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            parts.add("CASE")
                    .add(process(node.getOperand(), context));

            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, context));
            }

            node.getDefaultValue()
                    .ifPresent((value) -> parts.add("ELSE").add(process(value, context)));

            parts.add("END");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        @Override
        protected String visitWhenClause(WhenClause node, Void context) {
            return "WHEN " + process(node.getOperand(), context) + " THEN " + process(node.getResult(), context);
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " BETWEEN " +
                    process(node.getMin(), context) + " AND " + process(node.getMax(), context) + ")";
        }

        @Override
        protected String visitInPredicate(InPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " IN " + process(node.getValueList(), context) + ")";
        }

        @Override
        protected String visitInListExpression(InListExpression node, Void context) {
            return "(" + joinExpressions(node.getValues()) + ")";
        }

        private String visitFilter(Expression node, Void context) {
            return "(WHERE " + process(node, context) + ')';
        }

        @Override
        protected String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context) {
            return new StringBuilder()
                    .append("(")
                    .append(process(node.getValue(), context))
                    .append(' ')
                    .append(node.getOperator().getValue())
                    .append(' ')
                    .append(node.getQuantifier().toString())
                    .append(' ')
                    .append(process(node.getSubquery(), context))
                    .append(")")
                    .toString();
        }

        @Override
        protected String visitGroupingOperation(GroupingOperation node, Void context) {
            return "GROUPING (" + joinExpressions(node.getGroupingColumns()) + ")";
        }

        @Override
        protected String visitRowDataType(RowDataType node, Void context) {
            return node.getFields().stream()
                    .map(this::process)
                    .collect(joining(", ", "ROW(", ")"));
        }

        @Override
        protected String visitRowField(RowDataType.Field node, Void context) {
            StringBuilder result = new StringBuilder();

            if (node.getName().isPresent()) {
                result.append(process(node.getName().get(), context));
                result.append(" ");
            }

            result.append(process(node.getType(), context));

            return result.toString();
        }

        @Override
        protected String visitGenericDataType(GenericDataType node, Void context) {
            StringBuilder result = new StringBuilder();
            result.append(node.getName());

            if (!node.getArguments().isEmpty()) {
                result.append(node.getArguments().stream()
                        .map(this::process)
                        .collect(joining(", ", "(", ")")));
            }

            return result.toString();
        }

        @Override
        protected String visitTypeParameter(TypeParameter node, Void context) {
            return process(node.getValue(), context);
        }

        @Override
        protected String visitNumericTypeParameter(NumericParameter node, Void context) {
            return node.getValue();
        }

        @Override
        protected String visitIntervalDataType(IntervalDayTimeDataType node, Void context) {
            StringBuilder builder = new StringBuilder();

            builder.append("INTERVAL ");
            builder.append(node.getFrom());
            if (node.getFrom() != node.getTo()) {
                builder.append(" TO ")
                        .append(node.getTo());
            }

            return builder.toString();
        }

        @Override
        protected String visitDateTimeType(DateTimeDataType node, Void context) {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getType().toString().toLowerCase(Locale.ENGLISH)); // TODO: normalize to upper case according to standard SQL semantics
            if (node.getPrecision().isPresent()) {
                builder.append("(")
                        .append(node.getPrecision().get())
                        .append(")");
            }

            if (node.isWithTimeZone()) {
                builder.append(" with time zone"); // TODO: normalize to upper case according to standard SQL semantics
            }

            return builder.toString();
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right) {
            return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
        }

        private String joinExpressions(List<Expression> expressions) {
            return Joiner.on(", ").join(expressions.stream()
                    .map((e) -> process(e, null))
                    .iterator());
        }

        private String formatWindow(Window window) {
            if (window instanceof WindowReference) {
                return formatExpression(((WindowReference) window).getName(), reWriter, queryContext, models);
            }

            return formatWindowSpecification((WindowSpecification) window);
        }

        private String formatWindowSpecification(WindowSpecification windowSpecification) {
            List<String> parts = new ArrayList<>();

            if (windowSpecification.getExistingWindowName().isPresent()) {
                parts.add(formatExpression(windowSpecification.getExistingWindowName().get(), reWriter, queryContext, models));
            }
            if (!windowSpecification.getPartitionBy().isEmpty()) {
                parts.add("PARTITION BY " + windowSpecification.getPartitionBy().stream()
                        .map(i -> formatExpression(i, reWriter, queryContext, models))
                        .collect(joining(", ")));
            }
            if (windowSpecification.getOrderBy().isPresent()) {
                parts.add(formatOrderBy(windowSpecification.getOrderBy().get()));
            }
            if (windowSpecification.getFrame().isPresent()) {
                parts.add(formatFrame(windowSpecification.getFrame().get()));
            }

            return '(' + Joiner.on(' ').join(parts) + ')';
        }

        String formatOrderBy(OrderBy orderBy) {
            return "ORDER BY " + formatSortItems(orderBy.getSortItems());
        }

        private String formatSortItems(List<SortItem> sortItems) {
            return Joiner.on(", ").join(sortItems.stream()
                    .map(sortItemFormatterFunction())
                    .iterator());
        }

        private Function<SortItem, String> sortItemFormatterFunction() {
            return input -> {
                StringBuilder builder = new StringBuilder();

                builder.append(formatExpression(input.getSortKey(), reWriter, queryContext, models));

                switch (input.getOrdering()) {
                    case ASCENDING:
                        builder.append(" ASC");
                        break;
                    case DESCENDING:
                        builder.append(" DESC");
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
                }

                switch (input.getNullOrdering()) {
                    case FIRST:
                        builder.append(" NULLS FIRST");
                        break;
                    case LAST:
                        builder.append(" NULLS LAST");
                        break;
                    case UNDEFINED:
                        // no op
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
                }

                return builder.toString();
            };
        }

        private String formatGroupBy(List<GroupingElement> groupingElements, IQueryGeneratorContext bridge, List<Model> models) {
            ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

            for (GroupingElement groupingElement : groupingElements) {
                String result = "";
                if (groupingElement instanceof SimpleGroupBy) {
                    List<Expression> columns = groupingElement.getExpressions();
                    if (columns.size() == 1) {
                        result = formatExpression(getOnlyElement(columns), reWriter, bridge, models);
                    } else {
                        result = formatGroupingSet(columns);
                    }
                } else if (groupingElement instanceof GroupingSets) {
                    result = format("GROUPING SETS (%s)", Joiner.on(", ").join(
                            ((GroupingSets) groupingElement).getSets().stream()
                                    .map(i -> formatGroupingSet(i))
                                    .iterator()));
                } else if (groupingElement instanceof Cube) {
                    result = format("CUBE %s", formatGroupingSet(groupingElement.getExpressions()));
                } else if (groupingElement instanceof Rollup) {
                    result = format("ROLLUP %s", formatGroupingSet(groupingElement.getExpressions()));
                }
                resultStrings.add(result);
            }
            return Joiner.on(", ").join(resultStrings.build());
        }

        private String formatGroupingSet(List<Expression> groupingSet) {
            return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
                    .map(i -> formatExpression(i, reWriter, queryContext, models))
                    .iterator()));
        }

        private String formatFrame(WindowFrame windowFrame) {
            StringBuilder builder = new StringBuilder();

            builder.append(windowFrame.getType().toString())
                    .append(' ');

            if (windowFrame.getEnd().isPresent()) {
                builder.append("BETWEEN ")
                        .append(formatFrameBound(windowFrame.getStart()))
                        .append(" AND ")
                        .append(formatFrameBound(windowFrame.getEnd().get()));
            } else {
                builder.append(formatFrameBound(windowFrame.getStart()));
            }

            return builder.toString();
        }

        private String formatFrameBound(FrameBound frameBound) {
            switch (frameBound.getType()) {
                case UNBOUNDED_PRECEDING:
                    return "UNBOUNDED PRECEDING";
                case PRECEDING:
                    return formatExpression(frameBound.getValue().get(), reWriter, queryContext, models) + " PRECEDING";
                case CURRENT_ROW:
                    return "CURRENT ROW";
                case FOLLOWING:
                    return formatExpression(frameBound.getValue().get(), reWriter, queryContext, models) + " FOLLOWING";
                case UNBOUNDED_FOLLOWING:
                    return "UNBOUNDED FOLLOWING";
            }
            throw new IllegalArgumentException("unhandled type: " + frameBound.getType());
        }
    }

    static String formatStringLiteral(String s) {
        s = s.replace("'", "''");
        if (CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(s)) {
            return "'" + s + "'";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("U&'");
        PrimitiveIterator.OfInt iterator = s.codePoints().iterator();
        while (iterator.hasNext()) {
            int codePoint = iterator.nextInt();
            checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: %s", s);
            if (isAsciiPrintable(codePoint)) {
                char ch = (char) codePoint;
                if (ch == '\\') {
                    builder.append(ch);
                }
                builder.append(ch);
            } else if (codePoint <= 0xFFFF) {
                builder.append('\\');
                builder.append(format("%04X", codePoint));
            } else {
                builder.append("\\+");
                builder.append(format("%06X", codePoint));
            }
        }
        builder.append("'");
        return builder.toString();
    }


    private static boolean isAsciiPrintable(int codePoint) {
        return codePoint >= 0x20 && codePoint < 0x7F;
    }


}
