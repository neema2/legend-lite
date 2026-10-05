import com.legend.Compiler;
public class Probe {
  static void t(String label, String q) {
    String model = "Class demo::P { name: String[1]; age: Integer[1]; }\n";
    try { Compiler.query(Compiler.compileModel(model), q).expression(); System.out.println("ACCEPTED  " + label); }
    catch (Throwable e) {
      String m = String.valueOf(e.getMessage());
      System.out.println("rejected  " + label + "  :: " + m.split("\n")[0].substring(0, Math.min(95, m.split("\n")[0].length())));
    }
  }
  public static void main(String[] a) {
    // bare names that exist NOWHERE in legend-engine or legend-pure
    t("avg(...)",              "|demo::P.all()->map(p|$p.age)->avg()");
    t("maxDate/minDate",       "|maxDate(%2020-01-01, %2021-01-01)");
    t("divideRound",           "|divideRound(10, 3, 2)");
    t("isNumeric",             "|isNumeric('12')");
    t("otherwise",             "|otherwise('a', 'b')");
    t("notEqualAnsi",          "|notEqualAnsi(1, 2)");
    t("percentileCont",        "|demo::P.all()->map(p|$p.age)->percentileCont(0.5)");
    t("sub(decimal)",          "|sub(1.0d, 2.0d)");
    t("navigate",              "|navigate(1, 2, 3)");
    t("typeAsDeclared",        "|typeAsDeclared('x', 'y')");
    t("FQN meta::legend::lite::avg", "|meta::legend::lite::avg([1,2,3])");
    // control: a genuinely upstream function, and a genuine nonsense name
    t("CONTROL average()",     "|demo::P.all()->map(p|$p.age)->average()");
    t("CONTROL bogusFn()",     "|bogusFnThatCannotExist(1)");
  }
}
