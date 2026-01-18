package org.finos.legend.engine.codegen;

import org.finos.legend.pure.dsl.definition.ClassDefinition;
import org.finos.legend.pure.dsl.definition.ClassDefinition.DerivedPropertyDefinition;
import org.finos.legend.pure.dsl.definition.ClassDefinition.PropertyDefinition;
import org.finos.legend.pure.dsl.definition.EnumDefinition;
import org.finos.legend.pure.dsl.definition.PureDefinition;
import org.finos.legend.pure.dsl.definition.PureDefinitionParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generates Java source code from Pure definitions.
 * 
 * Produces:
 * - Java records from Pure Classes
 * - Java enums from Pure Enums
 * - Computed methods from derived properties
 * 
 * This is build-time code generation - no reflection required.
 * Safe for GraalVM native-image.
 * 
 * Example:
 * 
 * <pre>
 * JavaCodeGenerator gen = new JavaCodeGenerator();
 * String javaCode = gen.generateRecord(classDef);
 * </pre>
 */
public final class JavaCodeGenerator {

    private static final String INDENT = "    ";

    /**
     * Generates Java source code for all definitions in a Pure source file.
     * 
     * @param pureSource The Pure source code
     * @return List of generated Java source files (filename, content pairs)
     */
    public List<GeneratedFile> generate(String pureSource) {
        List<PureDefinition> definitions = PureDefinitionParser.parse(pureSource);

        return definitions.stream()
                .filter(def -> def instanceof ClassDefinition || def instanceof EnumDefinition)
                .map(this::generateDefinition)
                .collect(Collectors.toList());
    }

    private GeneratedFile generateDefinition(PureDefinition def) {
        return switch (def) {
            case ClassDefinition classDef -> new GeneratedFile(
                    classDef.simpleName() + ".java",
                    classDef.packagePath(),
                    generateRecord(classDef));
            case EnumDefinition enumDef -> new GeneratedFile(
                    enumDef.simpleName() + ".java",
                    enumDef.packagePath(),
                    generateEnum(enumDef));
            default -> throw new IllegalArgumentException("Unsupported definition type: " + def.getClass());
        };
    }

    /**
     * Generates a Java record from a Pure Class definition.
     */
    public String generateRecord(ClassDefinition classDef) {
        StringBuilder sb = new StringBuilder();

        // Package declaration
        if (!classDef.packagePath().isEmpty()) {
            sb.append("package ").append(classDef.packagePath().replace("::", ".")).append(";\n\n");
        }

        // Imports
        sb.append(generateImports(classDef));

        // Record declaration
        sb.append("/**\n");
        sb.append(" * Generated from Pure class: ").append(classDef.qualifiedName()).append("\n");
        sb.append(" */\n");
        sb.append("public record ").append(classDef.simpleName()).append("(\n");

        // Record components (properties)
        List<PropertyDefinition> props = classDef.properties();
        for (int i = 0; i < props.size(); i++) {
            PropertyDefinition prop = props.get(i);
            sb.append(INDENT).append(mapType(prop)).append(" ").append(prop.name());
            if (i < props.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(")");

        // Check if we need a body for derived properties
        if (!classDef.derivedProperties().isEmpty()) {
            sb.append(" {\n");

            // Generate derived property methods
            for (DerivedPropertyDefinition derived : classDef.derivedProperties()) {
                sb.append("\n");
                sb.append(INDENT).append("/**\n");
                sb.append(INDENT).append(" * Derived property: ").append(derived.expression()).append("\n");
                sb.append(INDENT).append(" */\n");
                sb.append(INDENT).append("public ").append(mapPureType(derived.type()));
                sb.append(" ").append(derived.name()).append("() {\n");
                sb.append(INDENT).append(INDENT).append("return ");
                sb.append(translateExpression(derived.expression()));
                sb.append(";\n");
                sb.append(INDENT).append("}\n");
            }

            sb.append("}\n");
        } else {
            sb.append(" {}\n");
        }

        return sb.toString();
    }

    /**
     * Generates a Java enum from a Pure Enum definition.
     */
    public String generateEnum(EnumDefinition enumDef) {
        StringBuilder sb = new StringBuilder();

        // Package declaration
        if (!enumDef.packagePath().isEmpty()) {
            sb.append("package ").append(enumDef.packagePath().replace("::", ".")).append(";\n\n");
        }

        // Enum declaration
        sb.append("/**\n");
        sb.append(" * Generated from Pure enum: ").append(enumDef.qualifiedName()).append("\n");
        sb.append(" */\n");
        sb.append("public enum ").append(enumDef.simpleName()).append(" {\n");

        // Enum values
        List<String> values = enumDef.values();
        for (int i = 0; i < values.size(); i++) {
            sb.append(INDENT).append(values.get(i));
            if (i < values.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }

        sb.append("}\n");

        return sb.toString();
    }

    /**
     * Generates necessary imports for a class.
     */
    private String generateImports(ClassDefinition classDef) {
        StringBuilder imports = new StringBuilder();

        boolean needsList = classDef.properties().stream()
                .anyMatch(p -> p.upperBound() == null);

        boolean needsOptional = classDef.properties().stream()
                .anyMatch(p -> isOptional(p) && !isPrimitiveType(p.type()));

        boolean needsOptionalInt = classDef.properties().stream()
                .anyMatch(p -> isOptional(p) && "Integer".equals(p.type()));

        boolean needsOptionalDouble = classDef.properties().stream()
                .anyMatch(p -> isOptional(p) && "Float".equals(p.type()));

        boolean needsOptionalLong = classDef.properties().stream()
                .anyMatch(p -> isOptional(p) && "Long".equals(p.type()));

        boolean needsLocalDate = classDef.properties().stream()
                .anyMatch(p -> "Date".equals(p.type()));

        boolean needsLocalDateTime = classDef.properties().stream()
                .anyMatch(p -> "DateTime".equals(p.type()));

        if (needsList) {
            imports.append("import java.util.List;\n");
        }
        if (needsOptional) {
            imports.append("import java.util.Optional;\n");
        }
        if (needsOptionalInt) {
            imports.append("import java.util.OptionalInt;\n");
        }
        if (needsOptionalDouble) {
            imports.append("import java.util.OptionalDouble;\n");
        }
        if (needsOptionalLong) {
            imports.append("import java.util.OptionalLong;\n");
        }
        if (needsLocalDate) {
            imports.append("import java.time.LocalDate;\n");
        }
        if (needsLocalDateTime) {
            imports.append("import java.time.LocalDateTime;\n");
        }

        if (imports.length() > 0) {
            imports.append("\n");
        }

        return imports.toString();
    }

    private boolean isOptional(PropertyDefinition p) {
        return p.lowerBound() == 0 && p.upperBound() != null && p.upperBound() == 1;
    }

    private boolean isPrimitiveType(String type) {
        return "Integer".equals(type) || "Float".equals(type) ||
                "Boolean".equals(type) || "Long".equals(type);
    }

    /**
     * Maps a Pure property to a Java type.
     * 
     * Multiplicity mapping:
     * - [1] → primitive (int, boolean, double) or non-null reference
     * - [0..1] → OptionalInt/OptionalDouble/OptionalLong for primitives,
     * Optional<T> for references
     * - [*] → List<T>
     */
    private String mapType(PropertyDefinition prop) {
        String baseType = mapPureType(prop.type());

        // Handle multiplicity
        if (prop.upperBound() == null) {
            // [*] or [1..*] → List<T>
            return "List<" + boxType(baseType) + ">";
        }

        if (prop.lowerBound() == 0) {
            // [0..1] → Use primitive optionals for primitive types
            return switch (prop.type()) {
                case "Integer" -> "OptionalInt";
                case "Float" -> "OptionalDouble";
                case "Long" -> "OptionalLong";
                default -> "Optional<" + boxType(baseType) + ">";
            };
        }

        // [1] → primitive for basic types, otherwise reference type
        return toPrimitiveIfPossible(baseType);
    }

    /**
     * Maps a Pure type name to a Java type (boxed/reference form).
     */
    private String mapPureType(String pureType) {
        return switch (pureType) {
            case "String" -> "String";
            case "Integer" -> "Integer";
            case "Float" -> "Double";
            case "Boolean" -> "Boolean";
            case "Date" -> "LocalDate";
            case "DateTime" -> "LocalDateTime";
            default -> pureType; // Custom type (enum or class reference)
        };
    }

    /**
     * Converts to primitive type if possible (for required [1] properties).
     */
    private String toPrimitiveIfPossible(String type) {
        return switch (type) {
            case "Integer" -> "int";
            case "Double" -> "double";
            case "Boolean" -> "boolean";
            case "Long" -> "long";
            default -> type; // String, LocalDate, custom types stay as-is
        };
    }

    /**
     * Boxes primitive types for use in generics.
     */
    private String boxType(String type) {
        return switch (type) {
            case "int" -> "Integer";
            case "double" -> "Double";
            case "boolean" -> "Boolean";
            case "long" -> "Long";
            default -> type;
        };
    }

    /**
     * Translates a Pure expression to Java.
     * 
     * Example: $this.firstName + ' ' + $this.lastName → firstName + " " + lastName
     */
    private String translateExpression(String pureExpr) {
        return pureExpr
                .replace("$this.", "") // Remove $this. prefix
                .replace("'", "\""); // Single quotes → double quotes
    }

    /**
     * Writes generated files to an output directory.
     */
    public void writeToDirectory(List<GeneratedFile> files, Path outputDir) throws IOException {
        for (GeneratedFile file : files) {
            Path packageDir = outputDir;
            if (!file.packageName().isEmpty()) {
                packageDir = outputDir.resolve(file.packageName().replace(".", "/").replace("::", "/"));
            }
            Files.createDirectories(packageDir);
            Files.writeString(packageDir.resolve(file.fileName()), file.content());
        }
    }

    /**
     * Represents a generated Java source file.
     */
    public record GeneratedFile(
            String fileName,
            String packageName,
            String content) {
    }
}
