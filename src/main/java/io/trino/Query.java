package io.trino;

import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.NamedClientTypeSignature;
import io.trino.client.RowFieldName;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.sql.ExpressionFormatter;
import io.trino.sql.tree.*;

import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.StandardTypes.*;

public class Query {
    static final boolean supportsParametricDateTime = false;

    public static String formatType(DataType type)
    {
        if (type instanceof DateTimeDataType) {
            DateTimeDataType dataTimeType = (DateTimeDataType) type;
            if (!supportsParametricDateTime) {
                if (dataTimeType.getType() == DateTimeDataType.Type.TIMESTAMP && dataTimeType.isWithTimeZone()) {
                    return TIMESTAMP_WITH_TIME_ZONE;
                }
                if (dataTimeType.getType() == DateTimeDataType.Type.TIMESTAMP && !dataTimeType.isWithTimeZone()) {
                    return TIMESTAMP;
                }
                if (dataTimeType.getType() == DateTimeDataType.Type.TIME && !dataTimeType.isWithTimeZone()) {
                    return TIME;
                }
                if (dataTimeType.getType() == DateTimeDataType.Type.TIME && dataTimeType.isWithTimeZone()) {
                    return TIMESTAMP_WITH_TIME_ZONE;
                }
            }

            return ExpressionFormatter.formatExpression(type);
        }
        if (type instanceof RowDataType) {
            RowDataType rowDataType = (RowDataType) type;
            return rowDataType.getFields().stream()
                    .map(field -> field.getName().map(name -> name + " ").orElse("") + formatType(field.getType()))
                    .collect(Collectors.joining(", ", ROW + "(", ")"));
        }
        if (type instanceof GenericDataType) {
            GenericDataType dataType = (GenericDataType) type;
            if (dataType.getArguments().isEmpty()) {
                return dataType.getName().getValue();
            }

            return dataType.getArguments().stream()
                    .map(parameter -> {
                        if (parameter instanceof NumericParameter) {
                            return ((NumericParameter) parameter).getValue();
                        }
                        if (parameter instanceof TypeParameter) {
                            return formatType(((TypeParameter) parameter).getValue());
                        }
                        throw new IllegalArgumentException("Unsupported parameter type: " + parameter.getClass().getName());
                    })
                    .collect(Collectors.joining(", ", dataType.getName().getValue() + "(", ")"));
        }
        if (type instanceof IntervalDayTimeDataType) {
            return ExpressionFormatter.formatExpression(type);
        }

        throw new IllegalArgumentException("Unsupported data type: " + type.getClass().getName());
    }

    public static ClientTypeSignature toClientTypeSignature(TypeSignature signature)
    {
        if (!supportsParametricDateTime) {
            if (signature.getBase().equalsIgnoreCase(TIMESTAMP)) {
                return new ClientTypeSignature(TIMESTAMP);
            }
            if (signature.getBase().equalsIgnoreCase(TIMESTAMP_WITH_TIME_ZONE)) {
                return new ClientTypeSignature(TIMESTAMP_WITH_TIME_ZONE);
            }
            if (signature.getBase().equalsIgnoreCase(TIME)) {
                return new ClientTypeSignature(TIME);
            }
            if (signature.getBase().equalsIgnoreCase(TIME_WITH_TIME_ZONE)) {
                return new ClientTypeSignature(TIME_WITH_TIME_ZONE);
            }
        }

        return new ClientTypeSignature(signature.getBase(), signature.getParameters().stream()
                .map(t -> toClientTypeSignatureParameter(t))
                .collect(toImmutableList()));
    }

    private static ClientTypeSignatureParameter toClientTypeSignatureParameter(TypeSignatureParameter parameter)
    {
        switch (parameter.getKind()) {
            case TYPE:
                return ClientTypeSignatureParameter.ofType(toClientTypeSignature(parameter.getTypeSignature()));
            case NAMED_TYPE:
                return ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(
                        parameter.getNamedTypeSignature().getFieldName().map(value ->
                                new RowFieldName(value.getName())),
                        toClientTypeSignature(parameter.getNamedTypeSignature().getTypeSignature())));
            case LONG:
                return ClientTypeSignatureParameter.ofLong(parameter.getLongLiteral());
            case VARIABLE:
                // not expected here
        }
        throw new IllegalArgumentException("Unsupported kind: " + parameter.getKind());
    }
}
