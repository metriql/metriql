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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.metriql.service.model.Model;
import com.metriql.warehouse.spi.querycontext.IQueryGeneratorContext;
import io.trino.sql.tree.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.ExpressionFormatter.formatGroupBy;
import static io.trino.sql.ExpressionFormatter.formatWindowSpecification;
import static io.trino.sql.MetriqlExpressionFormatter.formatExpression;
import static io.trino.sql.MetriqlExpressionFormatter.formatStringLiteral;
import static io.trino.sql.RowPatternFormatter.formatPattern;
import static java.util.stream.Collectors.joining;

public final class MetriqlSqlFormatter {
    private static final String INDENT = "   ";

    private MetriqlSqlFormatter() {
    }

    public static String formatSql(Node root, SqlToSegmentation reWriter, IQueryGeneratorContext context, List<Model> models) {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, reWriter, context, models).process(root, 0);
        return builder.toString();
    }

    static String formatName(QualifiedName name, SqlToSegmentation reWriter, IQueryGeneratorContext context, List<Model> models) {
        return name.getOriginalParts().stream()
                .map(exp -> formatExpression(exp, reWriter, context, models))
                .collect(joining("."));
    }

    private static class Formatter extends AstVisitor<Void, Integer> {
        private final StringBuilder builder;
        private final IQueryGeneratorContext context;
        private final List<Model> models;
        private final SqlToSegmentation reWriter;

        public Formatter(StringBuilder builder, SqlToSegmentation reWriter, IQueryGeneratorContext context, List<Model> models) {
            this.builder = builder;
            this.reWriter = reWriter;
            this.context = context;
            this.models = models;
        }

        @Override
        protected Void visitNode(Node node, Integer indent) {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent) {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node, reWriter, context, models));
            return null;
        }

        @Override
        protected Void visitRowPattern(RowPattern node, Integer indent) {
            checkArgument(indent == 0, "visitRowPattern should only be called at root");
            builder.append(formatPattern(node));
            return null;
        }

        @Override
        protected Void visitUnnest(Unnest node, Integer indent) {
            builder.append("UNNEST(")
                    .append(node.getExpressions().stream()
                            .map(i -> formatExpression(i, reWriter, context, models))
                            .collect(joining(", ")))
                    .append(")");
            if (node.isWithOrdinality()) {
                builder.append(" WITH ORDINALITY");
            }
            return null;
        }

        @Override
        protected Void visitLateral(Lateral node, Integer indent) {
            append(indent, "LATERAL (");
            process(node.getQuery(), indent + 1);
            append(indent, ")");
            return null;
        }

        @Override
        protected Void visitPrepare(Prepare node, Integer indent) {
            append(indent, "PREPARE ");
            builder.append(node.getName());
            builder.append(" FROM");
            builder.append("\n");
            process(node.getStatement(), indent + 1);
            return null;
        }

        @Override
        protected Void visitDeallocate(Deallocate node, Integer indent) {
            append(indent, "DEALLOCATE PREPARE ");
            builder.append(node.getName());
            return null;
        }

        @Override
        protected Void visitExecute(Execute node, Integer indent) {
            append(indent, "EXECUTE ");
            builder.append(node.getName());
            List<Expression> parameters = node.getParameters();
            if (!parameters.isEmpty()) {
                builder.append(" USING ");
                Joiner.on(", ").appendTo(builder, parameters);
            }
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent) {
            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, formatExpression(query.getName(), reWriter, context, models));
                    query.getColumnNames().ifPresent(columnNames -> appendAliasColumns(builder, columnNames, reWriter, context, models));
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            }

            processRelation(node.getQueryBody(), indent);

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getOffset().isPresent()) {
                process(node.getOffset().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                process(node.getLimit().get(), indent);
            }
            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent) {
            try {
                String proxyQuery = reWriter.convert(context, node);
                append(indent, proxyQuery);
            } catch (UnsupportedOperationException e) {
                internalVisitQuerySpecification(node, indent);
            }

            return null;
        }

        protected void internalVisitQuerySpecification(QuerySpecification node, Integer indent) {
            process(node.getSelect(), indent);

            if (node.getFrom().isPresent()) {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(node.getFrom().get(), indent);
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + ExpressionFormatter.formatExpression(node.getWhere().get()))
                        .append('\n');
            }

            if (node.getGroupBy().isPresent()) {
                append(indent, "GROUP BY " + (node.getGroupBy().get().isDistinct() ? " DISTINCT " : "") + formatGroupBy(node.getGroupBy().get().getGroupingElements())).append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + ExpressionFormatter.formatExpression(node.getHaving().get()))
                        .append('\n');
            }

            if (!node.getWindows().isEmpty()) {
                append(indent, "WINDOW");
                formatDefinitionList(node.getWindows().stream()
                        .map(definition -> ExpressionFormatter.formatExpression(definition.getName()) + " AS " + formatWindowSpecification(definition.getWindow()))
                        .collect(toImmutableList()), indent + 1);
            }

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getOffset().isPresent()) {
                process(node.getOffset().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                process(node.getLimit().get(), indent);
            }
        }

        @Override
        protected Void visitOrderBy(OrderBy node, Integer indent) {
            append(indent, new MetriqlExpressionFormatter.Formatter(reWriter, context, models).formatOrderBy(node))
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitOffset(Offset node, Integer indent) {
            append(indent, "OFFSET ")
                    .append(formatExpression(node.getRowCount(), reWriter, context, models))
                    .append(" ROWS\n");
            return null;
        }

        @Override
        protected Void visitFetchFirst(FetchFirst node, Integer indent) {
            append(indent, "FETCH FIRST " + node.getRowCount().map(count -> formatExpression(count, reWriter, context, models) + " ROWS ").orElse("ROW "))
                    .append(node.isWithTies() ? "WITH TIES" : "ONLY")
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitLimit(Limit node, Integer indent) {
            append(indent, "LIMIT ")
                    .append(formatExpression(node.getRowCount(), reWriter, context, models))
                    .append('\n');
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent) {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            } else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent) {
            builder.append(formatExpression(node.getExpression(), reWriter, context, models));
            if (node.getAlias().isPresent()) {
                builder.append(' ')
                        .append(formatExpression(node.getAlias().get(), reWriter, context, models));
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer context) {
            node.getTarget().ifPresent(value -> builder
                    .append(formatExpression(value, reWriter, this.context, models))
                    .append("."));
            builder.append("*");

            if (!node.getAliases().isEmpty()) {
                builder.append(" AS (")
                        .append(Joiner.on(", ").join(node.getAliases().stream()
                                .map(i -> formatExpression(i, reWriter, this.context, models))
                                .collect(toImmutableList())))
                        .append(")");
            }

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent) {
            builder.append(formatName(node.getName(), reWriter, context, models));

            return null;
        }

        @Override
        protected Void
        visitJoin(Join node, Integer indent) {
            throw new UnsupportedOperationException("Metriql doesn't support JOINs in ad-hoc queries. Please define the relationship in data model.");
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent) {
            processRelationSuffix(node.getRelation(), indent);

            builder.append(' ')
                    .append(formatExpression(node.getAlias(), reWriter, context, models));
            appendAliasColumns(builder, node.getColumnNames(), reWriter, context, models);

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indent) {
            processRelationSuffix(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType())
                    .append(" (")
                    .append(node.getSamplePercentage())
                    .append(')');

            return null;
        }

        private void processRelationSuffix(Relation relation, Integer indent) {
            if ((relation instanceof AliasedRelation) || (relation instanceof SampledRelation) || (relation instanceof PatternRecognitionRelation)) {
                builder.append("( ");
                process(relation, indent + 1);
                append(indent, ")");
            } else {
                process(relation, indent);
            }
        }

        @Override
        protected Void visitValues(Values node, Integer indent) {
            builder.append(" VALUES ");

            boolean first = true;
            for (Expression row : node.getRows()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                builder.append(formatExpression(row, reWriter, context, models));
                first = false;
            }
            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent) {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");

            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer indent) {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("UNION ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitExcept(Except node, Integer indent) {
            processRelation(node.getLeft(), indent);

            builder.append("EXCEPT ");
            if (!node.isDistinct()) {
                builder.append("ALL ");
            }

            processRelation(node.getRight(), indent);

            return null;
        }

        @Override
        protected Void visitIntersect(Intersect node, Integer indent) {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("INTERSECT ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitExplain(Explain node, Integer indent) {
            builder.append("EXPLAIN ");
            if (node.isAnalyze()) {
                builder.append("ANALYZE ");
            }

            List<String> options = new ArrayList<>();

            for (ExplainOption option : node.getOptions()) {
                if (option instanceof ExplainType) {
                    options.add("TYPE " + ((ExplainType) option).getType());
                } else if (option instanceof ExplainFormat) {
                    options.add("FORMAT " + ((ExplainFormat) option).getType());
                } else {
                    throw new UnsupportedOperationException("unhandled explain option: " + option);
                }
            }

            if (!options.isEmpty()) {
                builder.append("(");
                Joiner.on(", ").appendTo(builder, options);
                builder.append(")");
            }

            builder.append("\n");

            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitShowCatalogs(ShowCatalogs node, Integer context) {
            builder.append("SHOW CATALOGS");

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent((value) ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowSchemas(ShowSchemas node, Integer context) {
            builder.append("SHOW SCHEMAS");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent((value) ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowTables(ShowTables node, Integer context) {
            builder.append("SHOW TABLES");

            node.getSchema().ifPresent(value ->
                    builder.append(" FROM ")
                            .append(formatName(value, reWriter, this.context, models)));

            node.getLikePattern().ifPresent(value ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowCreate(ShowCreate node, Integer context) {
            if (node.getType() == ShowCreate.Type.TABLE) {
                builder.append("SHOW CREATE TABLE ")
                        .append(formatName(node.getName(), reWriter, this.context, models));
            } else if (node.getType() == ShowCreate.Type.VIEW) {
                builder.append("SHOW CREATE VIEW ")
                        .append(formatName(node.getName(), reWriter, this.context, models));
            } else if (node.getType() == ShowCreate.Type.MATERIALIZED_VIEW) {
                builder.append("SHOW CREATE MATERIALIZED VIEW ")
                        .append(formatName(node.getName(), reWriter, this.context, models));
            }
            return null;
        }

        @Override
        protected Void visitShowColumns(ShowColumns node, Integer context) {
            builder.append("SHOW COLUMNS FROM ")
                    .append(formatName(node.getTable(), reWriter, this.context, models));

            node.getLikePattern().ifPresent(value ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowFunctions(ShowFunctions node, Integer context) {
            builder.append("SHOW FUNCTIONS");

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            node.getEscape().ifPresent((value) ->
                    builder.append(" ESCAPE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        private String formatPropertiesMultiLine(List<Property> properties) {
            if (properties.isEmpty()) {
                return "";
            }

            String propertyList = properties.stream()
                    .map(element -> INDENT +
                            formatExpression(element.getName(), reWriter, context, models) + " = " +
                            formatExpression(element.getValue(), reWriter, context, models))
                    .collect(joining(",\n"));

            return "\nWITH (\n" + propertyList + "\n)";
        }

        private String formatPropertiesSingleLine(List<Property> properties) {
            if (properties.isEmpty()) {
                return "";
            }

            String propertyList = properties.stream()
                    .map(element -> formatExpression(element.getName(), reWriter, context, models) + " = " +
                            formatExpression(element.getValue(), reWriter, context, models))
                    .collect(joining(", "));

            return " WITH ( " + propertyList + " )";
        }

        private String formatColumnDefinition(ColumnDefinition column) {
            StringBuilder sb = new StringBuilder()
                    .append(formatExpression(column.getName(), reWriter, context, models))
                    .append(" ").append(column.getType());
            if (!column.isNullable()) {
                sb.append(" NOT NULL");
            }
            column.getComment().ifPresent(comment ->
                    sb.append(" COMMENT ").append(formatStringLiteral(comment)));
            sb.append(formatPropertiesSingleLine(column.getProperties()));
            return sb.toString();
        }

        @Override
        protected Void visitComment(Comment node, Integer context) {
            String comment = node.getComment().isPresent() ? formatStringLiteral(node.getComment().get()) : "NULL";

            switch (node.getType()) {
                case TABLE:
                    builder.append("COMMENT ON TABLE ")
                            .append(node.getName())
                            .append(" IS ")
                            .append(comment);
                    break;
                case COLUMN:
                    builder.append("COMMENT ON COLUMN ")
                            .append(node.getName())
                            .append(" IS ")
                            .append(comment);
                    break;
            }

            return null;
        }

        @Override
        protected Void visitAnalyze(Analyze node, Integer context) {
            builder.append("ANALYZE ")
                    .append(formatName(node.getTableName(), reWriter, this.context, models));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
            return null;
        }


        @Override
        protected Void visitCallArgument(CallArgument node, Integer indent) {
            if (node.getName().isPresent()) {
                builder.append(node.getName().get())
                        .append(" => ");
            }
            builder.append(formatExpression(node.getValue(), reWriter, context, models));

            return null;
        }

        @Override
        protected Void visitCall(Call node, Integer indent) {
            builder.append("CALL ")
                    .append(node.getName())
                    .append("(");

            Iterator<CallArgument> arguments = node.getArguments().iterator();
            while (arguments.hasNext()) {
                process(arguments.next(), indent);
                if (arguments.hasNext()) {
                    builder.append(", ");
                }
            }

            builder.append(")");

            return null;
        }

        @Override
        protected Void visitRow(Row node, Integer indent) {
            builder.append("ROW(");
            boolean firstItem = true;
            for (Expression item : node.getItems()) {
                if (!firstItem) {
                    builder.append(", ");
                }
                process(item, indent);
                firstItem = false;
            }
            builder.append(")");
            return null;
        }


        private void processRelation(Relation relation, Integer indent) {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(((Table) relation).getName())
                        .append('\n');
            } else {
                process(relation, indent);
            }
        }

        private StringBuilder append(int indent, String value) {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private static String indentString(int indent) {
            return Strings.repeat(INDENT, indent);
        }

        private void formatDefinitionList(List<String> elements, int indent) {
            if (elements.size() == 1) {
                builder.append(" ")
                        .append(getOnlyElement(elements))
                        .append("\n");
            } else {
                builder.append("\n");
                for (int i = 0; i < elements.size() - 1; i++) {
                    append(indent, elements.get(i))
                            .append(",\n");
                }
                append(indent, elements.get(elements.size() - 1))
                        .append("\n");
            }
        }
    }

    private static void appendAliasColumns(StringBuilder builder, List<Identifier> columns, SqlToSegmentation reWriter, IQueryGeneratorContext bridge, List<Model> models) {
        if ((columns != null) && (!columns.isEmpty())) {
            String formattedColumns = columns.stream()
                    .map(i -> formatExpression(i, reWriter, bridge, models))
                    .collect(Collectors.joining(", "));

            builder.append(" (")
                    .append(formattedColumns)
                    .append(')');
        }
    }
}
