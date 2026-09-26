"""The null gate's javac flags, for every target that loads //tools/nullaway:nullaway."""

# The Maven build's flags, minus the three that do not apply under Bazel
# (feasibility.md Q1): NO -Xplugin:ErrorProne (JavaBuilder instantiates Error
# Prone itself and rejects the Maven form), and NO -J--add-exports block
# (rules_java's BASE_JDK9_JVM_OPTS already passes every one of them).
NULLAWAY_OPTS = [
    "-XDcompilePolicy=simple",
    "--should-stop=ifError=FLOW",
    "-Xmaxerrs",
    "10000",
    "-XepDisableAllChecks",
    "-Xep:NullAway:ERROR",
    "-XepOpt:NullAway:AnnotatedPackages=com.legend",
    "-XepOpt:NullAway:CustomNullableAnnotations=com.legend.base.Nullable",
    "-XepOpt:NullAway:CustomNonnullAnnotations=com.legend.base.NonNull",
    "-XepOpt:NullAway:JSpecifyMode=true",
    "-XepOpt:NullAway:CheckOptionalEmptiness=true",
    "-XDaddTypeAnnotationsToSymbol=true",
]

NULLAWAY_PLUGIN = "//tools/nullaway:nullaway"
