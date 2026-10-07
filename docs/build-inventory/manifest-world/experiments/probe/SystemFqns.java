public class SystemFqns {
    public static void main(String[] a) {
        for (var el : com.legend.builtin.SystemMetamodel.elements()) System.out.println(el.qualifiedName());
    }
}
