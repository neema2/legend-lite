// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.compiler.element.ModelContext;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import com.legend.protocol.ModelComposer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * THE MODEL'S WRITER (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.6a): DataCube's Create a Class…, Create an
 * Association… and Create a Data Space…, each ONE call, atomic. The actions are applied to the project in memory, in
 * order -- each class ({@link CreateClass}), then each association ({@link CreateAssociation}), then the data space
 * ({@link CreateDataSpace}) -- and the whole result is checked ONCE: the project's files with the changes, and nothing
 * else (each class's source is its own, and no file may read it), compile, and each class made and each association's
 * navigation, both ways, plan. The answer is the net changes, in the SDLC's own shape ({@code {type: CREATE|MODIFY,
 * path, pureCode}}): each element printed by lite's model printer (upstream's composer, rule for rule) and read back as
 * itself; with what was made, and the associations the model suggests -- offered, never made. A refusal anywhere writes
 * nothing ({@link ModelException}, answered as a model that does not compile is).
 *
 * <p>Upstream has each piece and not the whole (the user's question, 2026-10-11): the relation's columns, reading and
 * printing elements, the compile; one Java path here, asked of the tab's module on every plane.
 */
public final class ModelChanges {

    /** The project as it was read, by path, and as the actions change it. */
    private final String project;
    private final Map<String, Json.Obj> original;
    private final Map<String, Json.Obj> elements;
    /** What the actions say beside their files (a sample's rows left out). */
    final List<String> notes = new ArrayList<>();

    private ModelChanges(String project) {
        this.project = project;
        this.original = elementsOf(project);
        this.elements = new LinkedHashMap<>(original);
    }

    // ---------------------------------------------------------------------
    // The routes' doors
    // ---------------------------------------------------------------------

    /** {@code createClass}: {@code {"project", "package", "class"}} -- one class ({@link CreateClass}'s request). */
    public static Map<String, Object> createClass(Json.Obj request) {
        return made(request, List.of(request.getObj("class")), List.of(), null);
    }

    /**
     * {@code createAssociation}: {@code {"project", "package", "classes"?, "association"?}} -- with no association,
     * nothing is made: the answer says what the model suggests, and what each class can be joined on (the window's
     * start).
     */
    public static Map<String, Object> createAssociation(Json.Obj request) {
        Json.Obj association = request.getObjOr("association", null);
        return made(request, items(request, "classes"), association == null ? List.of() : List.of(association), null);
    }

    /** {@code createDataSpace}: {@code {"project", "package", "classes"?, "associations"?, "dataSpace"}}. */
    public static Map<String, Object> createDataSpace(Json.Obj request) {
        return made(request, items(request, "classes"), items(request, "associations"), request.getObj("dataSpace"));
    }

    private static Map<String, Object> made(Json.Obj request, List<Json.Obj> classes, List<Json.Obj> associations,
            @com.legend.base.Nullable Json.Obj dataSpace) {
        String pkg = packagePath(request.getString("package"));
        ModelChanges m = new ModelChanges(PureV1Api.modelText(request.getObj("project")));
        List<CreateClass.Made> madeClasses = new ArrayList<>();
        for (Json.Obj c : classes) {
            madeClasses.add(CreateClass.apply(m, pkg, c, PureV1Api.modelText(c.getObj("source"))));
        }
        List<CreateAssociation.Made> madeAssociations = new ArrayList<>();
        for (Json.Obj a : associations) {
            madeAssociations.add(CreateAssociation.apply(m, pkg, a));
        }
        String space = dataSpace == null ? null : CreateDataSpace.apply(m, pkg, dataSpace);

        // THE WHOLE, CHECKED ONCE: the project's files with the changes, alone; each class read back, each association
        // walked both ways, through the model's mapping
        List<Map<String, Object>> changes = m.changes();
        String after = m.afterText();
        try {
            ModelContext ctx = com.legend.Compiler.compileModel(after);
            for (CreateClass.Made c : madeClasses) {
                com.legend.Compiler.query(ctx, "|" + c.query()).plan(c.runtime());
            }
            for (CreateAssociation.Made a : madeAssociations) {
                for (String walk : a.walks()) {
                    com.legend.Compiler.query(ctx, "|" + walk).plan(a.runtime());
                }
            }
        } catch (LegendCompileException e) {
            throw refused("the model would not compile: " + e.getMessage());
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("changes", changes);
        out.put("classes", madeClasses.stream().map(CreateClass.Made::json).toList());
        out.put("associations", madeAssociations.stream().map(CreateAssociation.Made::json).toList());
        if (space != null) {
            out.put("dataSpace", space);
        }
        out.put("suggestions", m.suggestions(pkg));
        out.put("joinable", m.joinable(pkg));
        out.put("notes", m.notes);
        return out;
    }

    /**
     * {@code importSchema}: {@code {"project", "package", "schema"}} -- a schema's classes and enumerations written as
     * Pure ({@link ImportSchema}'s request; docs/MODEL_IMPORT_STUDY_2026_10_20.md).
     */
    public static Map<String, Object> importSchema(Json.Obj request) {
        String pkg = packagePath(request.getString("package"));
        ModelChanges m = new ModelChanges(PureV1Api.modelText(request.getObj("project")));
        ImportSchema.Made made = ImportSchema.apply(m, pkg, request.getObj("schema"));
        return m.checked(List.of(), made.json());
    }

    /** {@code mapClass}: {@code {"project", "package", "class", "sql", ...}} -- a class given its rows by a SELECT
     *  ({@link MapClass}'s request). */
    public static Map<String, Object> mapClass(Json.Obj request) {
        String pkg = packagePath(request.getString("package"));
        ModelChanges m = new ModelChanges(PureV1Api.modelText(request.getObj("project")));
        MapClass.Made made = MapClass.apply(m, pkg, request);
        List<String[]> plans = new ArrayList<>();
        plans.add(new String[] {made.query(), made.runtime()});
        return m.checked(plans, made.json());
    }

    /** {@code joinAssociation}: {@code {"project", "package", "association", "ends"}} -- an association of the
     *  project joined ({@link JoinAssociation}'s request). */
    public static Map<String, Object> joinAssociation(Json.Obj request) {
        String pkg = packagePath(request.getString("package"));
        ModelChanges m = new ModelChanges(PureV1Api.modelText(request.getObj("project")));
        JoinAssociation.Made made = JoinAssociation.apply(m, pkg, request);
        List<String[]> plans = new ArrayList<>();
        for (String walk : made.walks()) {
            plans.add(new String[] {walk, made.runtime()});
        }
        return m.checked(plans, made.json());
    }

    /**
     * The answer of one action, checked ONCE as {@link #made} checks its own: the project's files with the changes
     * compile, and each of {@code plans} ({query, runtime}) plans; the changes, {@code made}'s fields, the notes.
     */
    private Map<String, Object> checked(List<String[]> plans, Map<String, Object> made) {
        List<Map<String, Object>> changes = changes();
        String after = afterText();
        try {
            ModelContext ctx = com.legend.Compiler.compileModel(after);
            for (String[] plan : plans) {
                com.legend.Compiler.query(ctx, "|" + plan[0]).plan(plan[1]);
            }
        } catch (LegendCompileException e) {
            throw refused("the model would not compile: " + e.getMessage());
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("changes", changes);
        out.putAll(made);
        out.put("notes", notes);
        return out;
    }

    private static List<Json.Obj> items(Json.Obj request, String key) {
        List<Json.Obj> out = new ArrayList<>();
        for (Json.Node n : arrOf(request, key).items()) {
            out.add((Json.Obj) n);
        }
        return out;
    }

    // ---------------------------------------------------------------------
    // The project, as the actions change it
    // ---------------------------------------------------------------------

    /** The element at {@code path} as the actions so far leave it, or null. */
    @com.legend.base.Nullable Json.Obj get(String path) {
        return elements.get(path);
    }

    /** Every element as the actions so far leave it. */
    Iterable<Json.Obj> all() {
        return elements.values();
    }

    /**
     * {@code element} made, or in place of the one of its path; read back as itself first ({@link #file}). An element
     * of another kind at its path is not written over, and no path is both an element and a package: legend-engine
     * refuses a project holding {@code trades::domain} beside {@code trades::domain::Trade} ("An element named 'domain'
     * already exists in the package 'trades'").
     */
    void put(Json.Obj element) {
        file(element);
        String path = pathOf(element);
        Json.Obj was = elements.get(path);
        if (was != null && !textOf(was, "_type").equals(textOf(element, "_type"))) {
            throw refused("'" + path + "' is already the project's " + kind(was) + ": name it otherwise");
        }
        for (String other : elements.keySet()) {
            if (other.startsWith(path + "::")) {
                throw refused("'" + path + "' is a package of the project (" + other + "): name it otherwise");
            }
            if (path.startsWith(other + "::")) {
                throw refused("'" + other + "' is the project's " + kind(elements.get(other)) + ", and so cannot be the "
                        + "package of " + path + ": name it otherwise");
            }
        }
        elements.put(path, element);
    }

    /** What kind of element {@code e} is, as a person names it. */
    private static String kind(@com.legend.base.Nullable Json.Obj e) {
        String type = e == null ? "" : textOf(e, "_type");
        return switch (type) {
            case "relational" -> "Database";
            case "dataElement" -> "data";
            case "dataSpace" -> "data space";
            default -> type;
        };
    }

    /** The net changes: each element the project does not hold made, each it holds otherwise changed, in order. */
    private List<Map<String, Object>> changes() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map.Entry<String, Json.Obj> e : elements.entrySet()) {
            Json.Obj was = original.get(e.getKey());
            if (e.getValue().equals(was)) {
                continue;
            }
            Map<String, Object> change = new LinkedHashMap<>();
            change.put("type", was == null ? "CREATE" : "MODIFY");
            change.put("path", e.getKey());
            change.put("pureCode", file(e.getValue()));
            out.add(change);
        }
        return out;
    }

    /**
     * The project's text with the changes made: each element a change replaces cut out where the grammar found it,
     * and each change's file after. Text, not the project printed whole: the model printer prints upstream's elements,
     * and a file of the project need not be one it prints byte for byte.
     */
    String afterText() {
        Set<String> replaced = new HashSet<>();
        List<String> files = new ArrayList<>();
        for (Map<String, Object> c : changes()) {
            if ("MODIFY".equals(c.get("type"))) {
                replaced.add((String) c.get("path"));
            }
            files.add((String) c.get("pureCode"));
        }
        List<int[]> cuts = new ArrayList<>();
        int[] lineStarts = lineStarts(project);
        for (Json.Node n : Json.parseObject(PmcdParser.parseDocument(project, true)).getArr("elements").items()) {
            Json.Obj e = (Json.Obj) n;
            if (!"sectionIndex".equals(e.getStringOr("_type", "")) && replaced.contains(pathOf(e))) {
                Json.Obj at = e.getObj("sourceInformation");
                // the grammar's columns count from 1, the end's inclusive
                cuts.add(new int[] {lineStarts[at.getInt("startLine") - 1] + at.getInt("startColumn") - 1,
                        lineStarts[at.getInt("endLine") - 1] + at.getInt("endColumn")});
            }
        }
        cuts.sort((x, y) -> Integer.compare(y[0], x[0]));
        StringBuilder out = new StringBuilder(project);
        for (int[] cut : cuts) {
            out.delete(cut[0], cut[1]);
        }
        for (String f : files) {
            out.append(f.startsWith("###") ? "\n" : "\n###Pure\n").append(f);
        }
        return out.toString();
    }

    /**
     * The one runtime of the project that names {@code mapping} (the place's: {@code <pkg>::runtime::Browser}); none,
     * or several, is refused by name.
     */
    String runtimeNaming(String mapping) {
        List<String> found = new ArrayList<>();
        for (Json.Obj e : elements.values()) {
            if (!"runtime".equals(e.getStringOr("_type", ""))) {
                continue;
            }
            for (Json.Node n : arrOf(objOf(e, "runtimeValue"), "mappings").items()) {
                if (mapping.equals(((Json.Obj) n).getStringOr("path", ""))) {
                    found.add(pathOf(e));
                }
            }
        }
        if (found.size() != 1) {
            throw refused(found.isEmpty() ? "no runtime of the project names " + mapping + ": make a class first"
                    : "several runtimes of the project name " + mapping + " (" + String.join(", ", found) + ")");
        }
        return found.get(0);
    }

    /**
     * What {@code cls} can be joined on, each name to its type's path: its properties, and the local properties its
     * relation mapping keeps (a key only: {@code +customerId: Integer[0..1]: customer_id}).
     */
    Map<String, String> keysOf(String cls) {
        Map<String, String> out = new LinkedHashMap<>();
        Json.Obj c = elements.get(cls);
        if (c == null || !"class".equals(c.getStringOr("_type", ""))) {
            return out;
        }
        for (Json.Node n : arrOf(c, "properties").items()) {
            Json.Obj p = (Json.Obj) n;
            out.put(p.getString("name"), typeOf(p));
        }
        for (Json.Obj e : elements.values()) {
            if (!"mapping".equals(e.getStringOr("_type", ""))) {
                continue;
            }
            for (Json.Node n : arrOf(e, "classMappings").items()) {
                Json.Obj cm = (Json.Obj) n;
                if (!cls.equals(cm.getStringOr("class", ""))) {
                    continue;
                }
                for (Json.Node pm : arrOf(cm, "propertyMappings").items()) {
                    Json.Obj local = objOf((Json.Obj) pm, "localMappingProperty");
                    if (local.has("type")) {
                        out.put(objOf((Json.Obj) pm, "property").getString("property"), local.getString("type"));
                    }
                }
            }
        }
        return out;
    }

    /**
     * WHAT EACH CLASS CAN BE JOINED ON: each class of {@code pkg}'s domain, each property and key only of it, its type,
     * whether the class shows it, and the column of the relation function it is read from -- what the caller asks
     * whether it is unique over the class's rows ({@code query.ts duplicateLambda}), DataCube in its tab.
     */
    List<Map<String, Object>> joinable(String pkg) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (String cls : classesOf(pkg)) {
            Set<String> shown = new HashSet<>();
            Json.Obj c = elements.get(cls);
            for (Json.Node n : arrOf(c == null ? Json.obj() : c, "properties").items()) {
                shown.add(((Json.Obj) n).getString("name"));
            }
            String function = "";
            Map<String, String> columns = new LinkedHashMap<>();
            for (Json.Obj e : elements.values()) {
                if (!"mapping".equals(e.getStringOr("_type", ""))) {
                    continue;
                }
                for (Json.Node n : arrOf(e, "classMappings").items()) {
                    Json.Obj cm = (Json.Obj) n;
                    if (!"relation".equals(cm.getStringOr("_type", "")) || !cls.equals(cm.getStringOr("class", ""))) {
                        continue;
                    }
                    String descriptor = textOf(objOf(cm, "relationFunction"), "path");
                    int open = descriptor.indexOf('(');
                    function = open < 0 ? descriptor : descriptor.substring(0, open);
                    for (Json.Node pm : arrOf(cm, "propertyMappings").items()) {
                        Json.Obj p = (Json.Obj) pm;
                        columns.put(textOf(objOf(p, "property"), "property"), textOf(p, "column"));
                    }
                }
            }
            List<Map<String, Object>> keys = new ArrayList<>();
            for (Map.Entry<String, String> k : keysOf(cls).entrySet()) {
                Map<String, Object> key = new LinkedHashMap<>();
                key.put("name", k.getKey());
                key.put("type", k.getValue());
                key.put("shown", shown.contains(k.getKey()));
                String column = columns.get(k.getKey());
                if (column != null && !column.isEmpty()) {
                    key.put("column", column);
                }
                keys.add(key);
            }
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("class", cls);
            if (!function.isEmpty()) {
                entry.put("function", function);
            }
            entry.put("keys", keys);
            out.add(entry);
        }
        return out;
    }

    /** The classes of {@code pkg}'s domain the project holds, by path. */
    List<String> classesOf(String pkg) {
        List<String> out = new ArrayList<>();
        for (Json.Obj e : elements.values()) {
            if ("class".equals(e.getStringOr("_type", "")) && (pkg + "::domain").equals(e.getStringOr("package", ""))) {
                out.add(pathOf(e));
            }
        }
        return out;
    }

    /** Whether an association of the project relates {@code a} and {@code b}. */
    boolean related(String a, String b) {
        for (Json.Obj e : elements.values()) {
            if (!"association".equals(e.getStringOr("_type", ""))) {
                continue;
            }
            Set<String> ends = new HashSet<>();
            for (Json.Node n : arrOf(e, "properties").items()) {
                ends.add(typeOf((Json.Obj) n));
            }
            if (ends.equals(Set.of(a, b))) {
                return true;
            }
        }
        return false;
    }

    /**
     * THE ASSOCIATIONS THE MODEL SUGGESTS (§15.6a): each key or property of a class named after another class and one
     * of its properties, of its type ({@code Trade.customerId}: {@code Customer.id}, or {@code Customer.customerId}, the
     * same name), between two classes no association relates yet -- offered, never made.
     */
    private List<Map<String, Object>> suggestions(String pkg) {
        List<Map<String, Object>> out = new ArrayList<>();
        List<String> classes = classesOf(pkg);
        for (String a : classes) {
            for (String b : classes) {
                if (a.equals(b) || related(a, b)) {
                    continue;
                }
                Json.Obj bc = elements.get(b);
                String named = lowerFirst(bc == null ? "" : bc.getString("name"));
                Map<String, String> bKeys = keysOf(b);
                for (Map.Entry<String, String> k : keysOf(a).entrySet()) {
                    for (Map.Entry<String, String> p : bKeys.entrySet()) {
                        // `Trade.customerId` names `Customer.id` (the class and its property), or `Customer.customerId`
                        // (the same name, the other class's: its own key, named after it)
                        boolean names = k.getKey().equals(named + upperFirst(p.getKey()))
                                || (k.getKey().equals(p.getKey()) && k.getKey().startsWith(named) && !k.getKey().equals(named));
                        if (names && k.getValue().equals(p.getValue())) {
                            Map<String, Object> s = new LinkedHashMap<>();
                            s.put("ends", List.of(Map.of("class", b, "on", p.getKey()), Map.of("class", a, "on", k.getKey())));
                            out.add(s);
                        }
                    }
                }
            }
        }
        return out;
    }

    /** {@code o}'s text at {@code key}, empty when it has none. */
    static String textOf(Json.Obj o, String key) {
        String v = o.getStringOr(key, null);
        return v == null ? "" : v;
    }

    /** A property's type's path, as its protocol names it ({@code genericType.rawType.fullPath}). */
    static String typeOf(Json.Obj property) {
        return textOf(objOf(objOf(property, "genericType"), "rawType"), "fullPath");
    }

    static String lowerFirst(String name) {
        return name.isEmpty() ? name : name.substring(0, 1).toLowerCase(Locale.ROOT) + name.substring(1);
    }

    static String upperFirst(String name) {
        return name.isEmpty() ? name : name.substring(0, 1).toUpperCase(Locale.ROOT) + name.substring(1);
    }

    // ---------------------------------------------------------------------
    // Elements and names: what every action writes with
    // ---------------------------------------------------------------------

    /**
     * The words Pure reads as literals, never as a name: a name spelled so cannot be written unquoted, and the model
     * printer -- upstream's composer, rule for rule ({@code PureGrammarComposerUtility.convertIdentifier}) -- writes it
     * unquoted, so a file holding one would not read back.
     */
    static final Set<String> LITERAL_WORDS = Set.of("true", "false");

    /** The model's elements, by path, as protocol (no source information). */
    static Map<String, Json.Obj> elementsOf(String model) {
        Map<String, Json.Obj> out = new LinkedHashMap<>();
        for (Json.Node n : Json.parseObject(PmcdParser.parseDocument(model, false)).getArr("elements").items()) {
            Json.Obj e = (Json.Obj) n;
            if (!"sectionIndex".equals(e.getStringOr("_type", ""))) {
                out.put(pathOf(e), e);
            }
        }
        return out;
    }

    /** An element's path as the SDLC files it: its package and name -- a function's name its signature's
     *  ({@code TradeFn__Relation_1_}), as the grammar names it. */
    static String pathOf(Json.Obj element) {
        String name = element.getString("name");
        return element.has("package") && !element.getString("package").isEmpty()
                ? element.getString("package") + "::" + name : name;
    }

    /** Where each line of {@code text} starts. */
    static int[] lineStarts(String text) {
        List<Integer> starts = new ArrayList<>(List.of(0));
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '\n') {
                starts.add(i + 1);
            }
        }
        return starts.stream().mapToInt(Integer::intValue).toArray();
    }

    /** The one element a text holds, as protocol. */
    static Json.Obj element(String text) {
        for (Json.Node n : Json.parseObject(PmcdParser.parseDocument(text, false)).getArr("elements").items()) {
            Json.Obj e = (Json.Obj) n;
            if (!"sectionIndex".equals(e.getStringOr("_type", ""))) {
                return e;
            }
        }
        throw new IllegalStateException("no element in: " + text);
    }

    /**
     * One element as its file holds it: its section's header, then the element, as the model printer prints it -- and
     * read back as itself, or refused: the printer writes a name as upstream's does, unquoted when it is letters and
     * digits, and a few such names Pure reads as words of its own ({@code let}, {@code true}), so a file holding one
     * would not read back.
     */
    static String file(Json.Obj element) {
        String printed = printed(element);
        String again;
        try {
            again = printed(element(printed));
        } catch (LegendCompileException e) {
            throw refused("the file " + pathOf(element) + " would not read back (" + e.getMessage() + "): a name in it "
                    + "Pure reads as a word of its own: name it otherwise");
        }
        if (!again.equals(printed)) {
            throw new IllegalStateException("the file " + pathOf(element) + " reads back otherwise:\n" + printed
                    + "\n-- read back --\n" + again);
        }
        return printed;
    }

    static String printed(Json.Obj element) {
        return ModelComposer.model(new Json.Obj(new LinkedHashMap<>(Map.of("_type", Json.str("data"), "elements",
                new Json.Arr(List.of(element))))));
    }

    /** {@code element} as the element {@code name} of {@code pkg}, {@code fields} put in it. */
    static Json.Obj renamed(Json.Obj element, String pkg, String name, Map<String, Json.Node> fields) {
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>(element.fields());
        out.putAll(fields);
        out.put("package", Json.str(pkg));
        out.put("name", Json.str(name));
        return new Json.Obj(out);
    }

    /** {@code o}'s object at {@code key}, empty when it has none. */
    static Json.Obj objOf(Json.Obj o, String key) {
        Json.Obj v = o.getObjOr(key, null);
        return v == null ? Json.obj() : v;
    }

    /** {@code o}'s array at {@code key}, empty when it has none. */
    static Json.Arr arrOf(Json.Obj o, String key) {
        Json.Arr v = o.getArrOr(key, null);
        return v == null ? Json.arr() : v;
    }

    static Json.Obj with(Json.Obj o, String key, Json.Node value) {
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>(o.fields());
        out.put(key, value);
        return new Json.Obj(out);
    }

    /**
     * Upstream's {@code meta::pure::functions::string::makeCamelCase} (legend-engine,
     * core/pure/corefunctions/stringExtension.pure): the parts between {@code _} and spaces, each after the first --
     * or every one, {@code upperFirst} -- led by its first letter upper-cased, the rest lower-cased when it is all upper
     * case and kept as it is when not ({@code TRADE_ID} is {@code tradeId}, {@code customerId} stays).
     */
    static String camelCase(String input, boolean upperFirst) {
        StringBuilder out = new StringBuilder();
        int index = 0;
        for (String part : input.split("[_ ]")) {
            if (part.isEmpty()) {
                continue;
            }
            String first = part.substring(0, 1);
            String rest = part.substring(1);
            String restCased = rest.equals(rest.toUpperCase(Locale.ROOT)) ? rest.toLowerCase(Locale.ROOT) : rest;
            out.append(upperFirst || index > 0 ? first.toUpperCase(Locale.ROOT) : first.toLowerCase(Locale.ROOT))
                    .append(restCased);
            index += 1;
        }
        return out.toString();
    }

    /** {@code name} as Pure holds a name, made total where upstream's rule is not: each character a name cannot hold
     *  made {@code _}, led by {@code _} when it would start with a digit, {@code _} when nothing is left, followed by
     *  {@code _} when it is a word Pure reads as a literal ({@code true_}). */
    static String identifier(String name) {
        String held = name.replaceAll("[^A-Za-z0-9_]", "_");
        if (held.isEmpty()) {
            return "_";
        }
        if (LITERAL_WORDS.contains(held)) {
            return held + "_";
        }
        return Character.isDigit(held.charAt(0)) ? "_" + held : held;
    }

    /** A name Pure holds unquoted: letters, digits and {@code _}, not led by a digit, and no literal's word. */
    static boolean isIdentifier(String name) {
        return name.matches("[A-Za-z_][A-Za-z0-9_]*") && !LITERAL_WORDS.contains(name);
    }

    static String packagePath(String pkg) {
        for (String part : pkg.split("::", -1)) {
            if (!isIdentifier(part)) {
                throw refused("a package is Pure names joined by :: ('" + pkg + "')");
            }
        }
        return pkg;
    }

    /** A name as a Pure quoted name: {@code 'Trade ID'}, its quote and backslash escaped. */
    static String quoted(String name) {
        return literal(name);
    }

    static String quotedIfNeeded(String name) {
        return isIdentifier(name) ? name : quoted(name);
    }

    /** A Pure string literal. */
    static String literal(String text) {
        return "'" + text.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")
                .replace("\t", "\\t") + "'";
    }

    static LegendCompileException refused(String message) {
        return new ModelException(LegendCompileException.Phase.MODEL, message);
    }
}
