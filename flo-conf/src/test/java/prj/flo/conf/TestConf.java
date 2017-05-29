package prj.flo.conf;

public class TestConf {
  
  public static void main(String[] args) {
    //java -Da1=lv1_a -Db3=lv1_bb -Db2=lv1_b

    o(Conf.keyEnd("a"));
    o(Conf.keyEnd("aa"));
    o(Conf.keyEnd("a/"));
    
    o(Conf.get("a1"));
    o(Conf.get("a9"));
    o(Conf.get("a9", "no value"));
    System.setProperty("a9", "new value");
    o(Conf.get("a9"));
    o(Conf.get("c2"));

    o(Conf.range("b", "c"));
    o(Conf.range(Conf.keyStart("b/"), Conf.keyEnd("b/")));
    o(Conf.range("", "B"));
    o(Conf.range("u", ""));
  }
  
  private static void o(Object o) {
    System.out.println(o);
  }

}
