package com.metriql.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.kotlin.KotlinModule;
import com.google.common.base.CaseFormat;
import com.google.common.base.Throwables;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class JsonHelper {
    private final static ObjectMapper mapper = new ObjectMapper();
    private final static ObjectMapper prettyMapper;

    private static final JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(false);

    private JsonHelper() {
    }

    public static String encode(Object obj, boolean prettyPrint) {
        try {
            return (prettyPrint ? prettyMapper : mapper).writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Object is not json serializable", e);
        }
    }

    public static String encode(Object obj) {
        return encode(obj, false);
    }

    public static byte[] encodeAsBytes(Object obj) {
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    public static ObjectNode generate(Map<String, List<String>> map) {
        ObjectNode obj = jsonObject();
        for (Map.Entry<String, List<String>> item : map.entrySet()) {
            String key = item.getKey();
            obj.put(key, item.getValue().get(0));
        }
        return obj;
    }

    public static ObjectNode jsonObject() {
        return jsonNodeFactory.objectNode();
    }

    public static ArrayNode jsonArray() {
        return jsonNodeFactory.arrayNode();
    }

    public static TextNode textNode(String value) {
        return jsonNodeFactory.textNode(value);
    }

    public static BinaryNode binaryNode(byte[] value) {
        return jsonNodeFactory.binaryNode(value);
    }

    public static BooleanNode booleanNode(boolean value) {
        return jsonNodeFactory.booleanNode(value);
    }

    public static NumericNode numberNode(Number value) {
        return jsonNodeFactory.numberNode((value instanceof Double || value instanceof Float) ?
                value.doubleValue() : value.longValue());
    }

    public static <T extends JsonNode> T readSafe(String json)
            throws IOException {
        return (T) mapper.readTree(json);
    }

    public static <T extends JsonNode> T readSafe(byte[] json)
            throws IOException {
        return (T) mapper.readTree(json);
    }

    public static <T extends JsonNode> T read(String json) {
        try {
            return (T) mapper.readTree(json);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T read(byte[] json, TypeReference<T> typeReference) {
        try {
            return (T) mapper.readValue(json, typeReference);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T readSafe(String json, TypeReference<T> typeReference)
            throws IOException {
        return (T) mapper.readValue(json, typeReference);
    }

    public static <T> T read(String json, TypeReference<T> typeReference) {
        try {
            return (T) mapper.readValue(json, typeReference);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T extends JsonNode> T read(byte[] json) {
        try {
            return (T) mapper.readTree(json);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T readSafe(String json, Class<T> clazz)
            throws IOException {
        return mapper.readValue(json, clazz);
    }

//    inline fun <reified T> read(data: String): T {
//        return mapper.readValue(data, T::class.java)
//    }

    public static <T> T readSafe(byte[] json, Class<T> clazz)
            throws IOException {
        return mapper.readValue(json, clazz);
    }

    public static <T> T readSafe(InputStream json, Class<T> clazz)
            throws IOException {
        return mapper.readValue(json, clazz);
    }

    public static <T> T read(InputStream json, Class<T> clazz)
            throws IOException {
        return mapper.readValue(json, clazz);
    }

    public static <T> T read(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T convert(Object json, Class<T> clazz) {
        try {
            return mapper.convertValue(json, clazz);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T convert(Object json, JavaType clazz) {
        try {
            return mapper.convertValue(json, clazz);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T convert(Object json, Class<T> clazz, String errorMessage) {
        try {
            return mapper.convertValue(json, clazz);
        } catch (IllegalArgumentException e) {
            throw new MetriqlException(errorMessage + " : " + e.getMessage(), HttpResponseStatus.BAD_REQUEST);
        }
    }

    public static <T> T convert(Object json, TypeReference<T> ref) {
        try {
            return mapper.convertValue(json, ref);
        } catch (IllegalArgumentException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> T read(byte[] json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

    public static void register(ObjectMapper mapper, Boolean encodeEnumAsSnakeCase) {
        mapper.registerModule(new KotlinModule());
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new SimpleModule("typeInference", Version.unknownVersion()) {
            @Override
            public void setupModule(SetupContext context) {
                context.insertAnnotationIntrospector(new PolymorphicTypeStrAnnotationIntrospector());
                context.insertAnnotationIntrospector(new PolymorphicTypeIntAnnotationIntrospector());
                context.insertAnnotationIntrospector(new CamelCaseEnumAnnotationIntrospector(encodeEnumAsSnakeCase));
            }
        });

        mapper.setPropertyNamingStrategy(new PropertyNamingStrategy() {
            @Override
            public String nameForGetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
                if (method.hasReturnType() && (method.getRawReturnType() == Boolean.class || method.getRawReturnType() == boolean.class)
                        && method.getName().startsWith("is")) {
                    return method.getName();
                }
                return super.nameForGetterMethod(config, method, defaultName);
            }
        });

        // TODO: change this
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false);

        mapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);

        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX"));
    }

    static {
        register(mapper, false);
        prettyMapper = mapper.copy();
        prettyMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    private static class PolymorphicTypeAnnotationIntrospector extends AnnotationIntrospector {
        protected TypeResolverBuilder<?> typeResolver(String externalProperty) {
            if (externalProperty == null) {
                return null;
            }
            StdTypeResolverBuilder b = new StdTypeResolverBuilder();
            b = b.init(JsonTypeInfo.Id.NAME, null);
            b = b.inclusion(JsonTypeInfo.As.EXTERNAL_PROPERTY);
            b = b.typeProperty(externalProperty);
            return b;
        }

        @Override
        public Version version() {
            return Version.unknownVersion();
        }

    }

    private static class PolymorphicTypeStrAnnotationIntrospector extends PolymorphicTypeAnnotationIntrospector {
        @Override
        public Version version() {
            return Version.unknownVersion();
        }

        @Override
        public TypeResolverBuilder<?> findTypeResolver(MapperConfig<?> config, AnnotatedClass ac, JavaType baseType) {
            PolymorphicTypeStr annotation = ac.getAnnotation(PolymorphicTypeStr.class);
            return typeResolver(annotation == null ? null : annotation.externalProperty());
        }

        @Override
        public TypeResolverBuilder<?> findPropertyTypeResolver(MapperConfig<?> config, AnnotatedMember am, JavaType baseType) {
            PolymorphicTypeStr annotation = am.getAnnotation(PolymorphicTypeStr.class);
            return typeResolver(annotation == null ? null : annotation.externalProperty());
        }


        @Override
        public List<NamedType> findSubtypes(Annotated a) {
            PolymorphicTypeStr annotation = a.getAnnotation(PolymorphicTypeStr.class);
            if (annotation == null) {
                return null;
            }


            Class aClass = annotation.valuesEnum();
            for (Method method : aClass.getMethods()) {
                if (method.isAnnotationPresent(JsonCreator.class)) {
                    throw new IllegalStateException("Polymorphic types don't support custom Jackson creators.");
                }
            }

            StrValueEnum[] enumConstants = (StrValueEnum[]) aClass.getEnumConstants();
            List result = new ArrayList(enumConstants.length);

            boolean annotationPresent = aClass.isAnnotationPresent(UppercaseEnum.class);

            for (StrValueEnum enumConstant : enumConstants) {
                if (!(enumConstant instanceof Enum)) {
                    throw new IllegalStateException("UppercaseEnum can only be used in enums.");
                }

                String systemValue = ((Enum) enumConstant).name();

                if (annotation.isNamed()) {
                    result.add(new NamedType(enumConstant.getValueClass(annotation.name()), systemValue));
                } else {
                    result.add(new NamedType(enumConstant.getValueClass(), systemValue));
                }

                if (annotationPresent) {
                    String upperCamel = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, systemValue);

                    if (annotation.isNamed()) {
                        result.add(new NamedType(enumConstant.getValueClass(annotation.name()), upperCamel));
                    } else {
                        result.add(new NamedType(enumConstant.getValueClass(), upperCamel));
                    }

                }
            }

            return result;
        }


        @Override
        public boolean isAnnotationBundle(Annotation ann) {
            return ann.annotationType() == PolymorphicTypeStr.class;
        }
    }

    private static class PolymorphicTypeIntAnnotationIntrospector extends PolymorphicTypeAnnotationIntrospector {
        @Override
        public Version version() {
            return Version.unknownVersion();
        }

        @Override
        public TypeResolverBuilder<?> findTypeResolver(MapperConfig<?> config, AnnotatedClass ac, JavaType baseType) {
            PolymorphicTypeInt annotation = ac.getAnnotation(PolymorphicTypeInt.class);
            return typeResolver(annotation == null ? null : annotation.externalProperty());
        }

        @Override
        public TypeResolverBuilder<?> findPropertyTypeResolver(MapperConfig<?> config, AnnotatedMember am, JavaType baseType) {
            PolymorphicTypeInt annotation = am.getAnnotation(PolymorphicTypeInt.class);
            return typeResolver(annotation == null ? null : annotation.externalProperty());
        }

        @Override
        public List<NamedType> findSubtypes(Annotated a) {
            PolymorphicTypeInt annotation = a.getAnnotation(PolymorphicTypeInt.class);
            if (annotation == null) {
                return null;
            }

            Class aClass = annotation.valuesEnum();
            IntValueEnum[] enumConstants = (IntValueEnum[]) aClass.getEnumConstants();
            List result = new ArrayList(enumConstants.length);

            for (IntValueEnum enumConstant : enumConstants) {
                result.add(new NamedType(enumConstant.getIntValueClass(), String.valueOf(enumConstant.getName())));
            }

            return result;
        }


        @Override
        public boolean isAnnotationBundle(Annotation ann) {
            return ann.annotationType() == PolymorphicTypeInt.class;
        }
    }

    private static class CamelCaseEnumAnnotationIntrospector extends AnnotationIntrospector {
        private final Boolean encodeEnumAsSnakeCase;

        public CamelCaseEnumAnnotationIntrospector(Boolean encodeEnumAsSnakeCase) {
            this.encodeEnumAsSnakeCase = encodeEnumAsSnakeCase;
        }

        @Override
        public Version version() {
            return Version.unknownVersion();
        }

        @Override
        public Object findSerializer(Annotated am) {
            Annotation annotation = am.getAnnotation(UppercaseEnum.class);
            if (annotation == null) {
                return null;
            }

            if (!am.getType().isEnumType()) {
                throw new IllegalStateException("@UppercaseEnum can only be used in enums");
            }

            return new JsonSerializer<Enum>() {

                @Override
                public void serialize(Enum value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
                    CaseFormat caseFormat;
                    if (encodeEnumAsSnakeCase) {
                        caseFormat = CaseFormat.LOWER_UNDERSCORE;
                    } else {
                        caseFormat = CaseFormat.LOWER_CAMEL;
                    }
                    String lowerCamelValue = CaseFormat.UPPER_UNDERSCORE.to(caseFormat, value.name());
                    jsonGenerator.writeString(lowerCamelValue);
                }
            };
        }

        @Override
        public Object findDeserializer(Annotated am) {
            Annotation annotation = am.getAnnotation(UppercaseEnum.class);
            if (annotation == null) {
                return null;
            }

            if (!am.getType().isEnumType()) {
                throw new IllegalStateException("@UppercaseEnum can only be used in enums");
            }

            return new JsonDeserializer() {
                private Object checkForName(Enum<?>[] values, String name) {
                    for (Enum<?> value : values) {
                        if (value.name().equals(name)) {
                            return value;
                        }
                    }

                    return null;
                }

                @Override
                public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                    Enum<?>[] values = (Enum<?>[]) am.getType().getRawClass().getEnumConstants();

                    String name = p.getText().toUpperCase(Locale.ENGLISH);
                    // TODO: find a more reliable way to see if the value is null
                    if(name.equals("NULL")) {
                        return null;
                    }
                    Object value = checkForName(values, name);
                    if (value != null) {
                        return value;
                    }

                    String fromCamelCase = CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, p.getText());
                    value = checkForName(values, fromCamelCase);
                    if (value != null) {
                        return value;
                    }

                    if (!ctxt.getConfig().hasDeserializationFeatures(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE.getMask())) {
                        return null;
                    }

                    throw new MetriqlException(
                            String.format("`%s` is not valid, valid values are %s",
                                    p.getText(),
                                    Arrays.stream(values).map(v -> "`" + v.name() + "`").collect(Collectors.joining(", "))), HttpResponseStatus.BAD_REQUEST
                    );
                }
            };
        }

        @Override
        public boolean isAnnotationBundle(Annotation ann) {
            return ann.annotationType() == UppercaseEnum.class;
        }
    }

    abstract public static class SimpleTypeResolver implements TypeIdResolver {

        @Override
        public void init(JavaType baseType) {

        }

        @Override
        public String idFromValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String idFromValueAndType(Object value, Class<?> suggestedType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String idFromBaseType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getDescForKnownTypeIds() {
            throw new UnsupportedOperationException();
        }

        @Override
        public JsonTypeInfo.Id getMechanism() {
            throw new UnsupportedOperationException();
        }
    }


}
