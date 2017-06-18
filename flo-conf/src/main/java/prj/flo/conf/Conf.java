package prj.flo.conf;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Configuration.
 * 
 * @author Leon Dong
 */
public class Conf {

  private static final String KeyEnv            = "flo_env";
  private static final String KeyConfFile       = "flo_conf_file";

  private static ConcurrentHashMap<String, SortedMap<String,String>>
      cacheOfClasspathFile =
      new ConcurrentHashMap<String, SortedMap<String,String>>();
  private static ConcurrentHashMap<String, SortedMap<String,String>>
      cacheOfFile =
      new ConcurrentHashMap<String, SortedMap<String,String>>();
  
  private static final String ConfClasspathFile = "flo.conf";
  private static final String ConfFile
      = single(KeyConfFile, true, true, null, null, null);

  private static final String Env = get(KeyEnv);




  /** Key value */
  public  static class Kv {
    
    public  String key;
    public  String value;
    
    public Kv() {}
    
    public Kv(String key, String value) {
      this.key   = key;
      this.value = value;
    }
    
    public String toString() {
      return "{" + key + " = " + value + "}";
    }
    
  }
  
  public  static class Event {

    public  static final int TypeModify = 1;
    public  static final int TypeDelete = 2;
    
    public  int    type;
    public  Kv     kv;
    public  Kv     preKv;
    
  }

  public  static interface Watcher {
    void changed(List<Event> events);
  }

  public  static String   env() {
    return Env;
  }

  public  static String   get(String key, String defaultValue) {
    return single(key, true, true, ConfFile, ConfClasspathFile, defaultValue);
  }

  public  static String   get(String key) {
    return get(key, null);
  }

  public  static Integer  getInt(String key, Integer defaultValue) {
    return str2int(get(key), defaultValue);
  }

  public  static Integer  getInt(String key) {
    return getInt(key, null);
  }

  public  static String   keyStart(String prefix) {
    if (prefix == null) {return null;}
    if (prefix.isEmpty()) {return "";}
    return prefix + (char)1;
  }
  
  public  static String   keyEnd(String prefix) {
    if (prefix == null) {return null;}
    if (prefix.isEmpty()) {return "";}
    int len = prefix.length();
    return prefix.substring(0, len - 1) + (char)(prefix.charAt(len - 1) + 1);
  }
  
  public  static List<Kv> range(String key, String keyEnd) {
    return range(key, keyEnd, true, true, ConfFile, ConfClasspathFile);
  }
  
  public  static void     watch(String key, String keyEnd, Watcher watcher) {
    
  }

  public  static void     set(
      String key, String value, boolean expireWhenDisconnect) {
    //TODO set
  }
  



  private static String   single(
    String key,
    boolean getInSystemProperty, boolean getInSystemEnv,
    String getInFile, String getInClasspathFile,
    String defaultValue) {

    if (key == null) {return null;}

    String v = null;

    if (getInSystemProperty) {
      v = System.getProperty(key);
      if (v != null) {return v;}
    }

    if (getInSystemEnv) {
      v = System.getenv(key);
      if (v != null) {return v;}
    }

    if (getInFile != null) {
      v = singleInFile(getInFile, key);
      if (v != null) {return v;}
    }

    if (getInClasspathFile != null) {
      if (Env != null) {
        v = singleInClasspathFile(getInClasspathFile + "." + Env, key);
        if (v != null) {return v;}
      }
      v = singleInClasspathFile(getInClasspathFile, key);
      if (v != null) {return v;}
    }
    
    return defaultValue;
  }
  
  private static String   singleInClasspathFile(String path, String key) {
    return cacheOfClasspathFile(path).get(key);
  }

  private static String   singleInFile(String path, String key) {
    return cacheOfFile(path).get(key);
  }


  private static List<Kv> range(
    String keyStart, String keyEnd,
    boolean getInSystemProperty, boolean getInSystemEnv,
    String getInFile, String getInClasspathFile) {

    //priority:
    //systemProperty > systemEnv > file > classpath-file > defaultValue
    
    if (keyStart == null) {return null;}
    
    //single key
    if (keyEnd == null) {
      String v = single(keyStart, getInSystemProperty, getInSystemEnv,
          getInFile, getInClasspathFile, null);
      if (v == null) {return Collections.emptyList();}
      return Arrays.asList(new Kv[]{new Kv(keyStart, v)});
    }
    
    //range keys
    TreeMap<String, String> kvs = new TreeMap<String, String>();
    
    if (getInSystemProperty) {
      rangeInSysProperty(keyStart, keyEnd, kvs);
    }

    if (getInSystemEnv) {
      rangeInSysEnv(keyStart, keyEnd, kvs);
    }

    if (getInFile != null) {
      rangeInFile(getInFile, keyStart, keyEnd, kvs);
    }

    if (getInClasspathFile != null) {
      if (Env != null) {
        rangeInClasspathFile(
            getInClasspathFile + "." + Env, keyStart, keyEnd, kvs);
      }
      rangeInClasspathFile(getInClasspathFile, keyStart, keyEnd, kvs);
    }
    
    ArrayList<Kv> kvs2 = new ArrayList<Kv>(kvs.size());
    for (Entry<String,String> e : kvs.entrySet()) {
      kvs2.add(new Kv(e.getKey(), e.getValue()));
    }
    return kvs2;
  }
  
  private static Map<String,String> rangeInSysProperty(
      String keyStart, String keyEnd, Map<String, String> to) {
    return rangeInMap(System.getProperties(), keyStart, keyEnd, to);
  }
  
  private static Map<String,String> rangeInSysEnv(
      String keyStart, String keyEnd, Map<String, String> to) {
    return rangeInMap(System.getenv(), keyStart, keyEnd, to);
  }
  
  private static Map<String,String> rangeInClasspathFile(
      String path, String keyStart, String keyEnd, Map<String, String> to) {
    return rangeInMap(cacheOfClasspathFile(path), keyStart, keyEnd, to);
  }
  
  private static Map<String,String> rangeInFile(
      String path, String keyStart, String keyEnd, Map<String, String> to) {
    return rangeInMap(cacheOfFile(path), keyStart, keyEnd, to);
  }
  
  @SuppressWarnings("unchecked")
  private static Map<String,String> rangeInMap(
      Properties p, String keyStart, String keyEnd, Map<String, String> to) {
    return rangeInMap(
        new TreeMap<String,String>((Map<String,String>)((Map<?,?>)p)),
        keyStart, keyEnd, to);
  }
  
  private static Map<String,String> rangeInMap(
      Map<String,String> m, String keyStart, String keyEnd,
      Map<String, String> to) {
    return rangeInMap(
        new TreeMap<String,String>(m),
        keyStart, keyEnd, to);
  }
  
  private static Map<String,String> rangeInMap(
      SortedMap<String, String> m, String keyStart, String keyEnd,
      Map<String, String> to){

    if (to == null) {
      to = new TreeMap<String, String>();
    }
    
    
    //single {key}
    if (keyEnd == null) {
      String v = m.get(keyStart);
      if (v != null) {
        if (to.get(keyStart) == null) {
          to.put(keyStart, v);
        }
      }
      return to;
    }
    
    
    //range
    //key    == "" means start from the first
    //keyEnd == "" means end at the last
    boolean start = keyStart.length() == 0;
    boolean endAtLast = keyEnd.length() == 0;
    
    for (Entry<String,String> e : m.entrySet()) {
      if (!start) {
        start = e.getKey().compareTo(keyStart) >= 0;
        if (!start) {continue;}
      }

      if (!endAtLast) {
        if (e.getKey().compareTo(keyEnd) >= 0) {break;}
      }

      if (to.get(e.getKey()) == null) {
        to.put(e.getKey(), e.getValue());
      }
    }
    
    return to;
  }


  private static SortedMap<String,String> cacheOfClasspathFile(String path) {

    SortedMap<String,String> m = cacheOfClasspathFile.get(path);
    if (m != null) {return m;}

    Properties p = new Properties();
    try {
      p.load(Conf.class.getClassLoader().getResourceAsStream(path));
    } catch (IOException e) {}
    @SuppressWarnings("unchecked")
    SortedMap<String,String> m2 =
        new TreeMap<String,String>(((Map<String,String>)((Map<?,?>)p)));

    SortedMap<String,String> m1 = cacheOfClasspathFile.putIfAbsent(path, m2);
    return m1 == null ? m2 : m1;
  }
  
  private static SortedMap<String,String> cacheOfFile(String path) {

    SortedMap<String,String> m = cacheOfFile.get(path);
    if (m != null) {return m;}

    Properties p = new Properties();
    try {
      p.load(new FileInputStream(path));
    } catch (IOException e) {}
    @SuppressWarnings("unchecked")
    SortedMap<String,String> m2 =
        new TreeMap<String,String>(((Map<String,String>)((Map<?,?>)p)));

    SortedMap<String,String> m1 = cacheOfFile.putIfAbsent(path, m2);
    return m1 == null ? m2 : m1;
  }


  private static Integer str2int(String str, Integer defaultValue) {
    if (str == null) {return defaultValue;}

    try {
      return Integer.parseInt(str);

    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

}
