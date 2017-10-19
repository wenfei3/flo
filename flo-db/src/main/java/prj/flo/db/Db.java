package prj.flo.db;

import java.io.StringReader;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import prj.flo.conf.Conf;

/**
 * Database framework.
 * 
 * @author Leon Dong
 */
public class Db {

  //Part 1: databases utility

  //init as no more then 8 databases usually
  private static final ConcurrentHashMap<String, Db> dbs
      = new ConcurrentHashMap<String, Db>(16, 0.9f, 16);
  private static final ConcurrentHashMap<String, Object> dbLocks
      = new ConcurrentHashMap<String, Object>(16, 0.9f, 16);

  public  static Db getInstance(String name) {
    Db db = dbs.get(name);
    if (db == null) {
      Object lock = getDbLock(name);
      synchronized (lock) {
        db = dbs.get(name);
        if (db == null) {

          db = new Db(name);
          dbs.put(name, db);

        }
      }
    }
    return db;
  }
  
  private static Object getDbLock(String name) {
      Object lock = dbLocks.get(name);
      if (lock == null) {
          lock = new Object();
          Object lock2 = dbLocks.putIfAbsent(name, lock);
          if (lock2 != null) {lock = lock2;}
      }
      return lock;
  }
















  //Part 2: database server pool.
  //one Db instance for one pool of db servers.
  //each db server has a pool of connections.
  
  private static final String ConfKeyPrefix = "flo_db_conf_key_prefix";
  
  private static class Server {

    public  static Server valueOf(String str) {
      /*
      driver=mysql    (optional, default:mysql)
      host=10.99.184.156
      port=3306       (optional, default:3306)
      database=test1
      user=root
      password=
      replica=master  (optional, default:master)
      poolSizeMin=1   (optional, default:1)
      poolSizeMax=32  (optional)
      poolSizeIncre=1 (optional, default:1)
      idleMax=600     (optional, default:600, unit:second)
       */
      if (str == null) {return null;}
      try {
        Server s = new Server();
        PropertyReader pr = new PropertyReader(str);
        s.driver    = pr.get("driver");
        s.host      = pr.get("host");
        s.port      = pr.getInt("port");
        s.database  = pr.get("database");
        s.user      = pr.get("user");
        s.password  = pr.get("password");
        s.replica   = pr.get("replica");
        s.poolMin   = pr.getInt("poolMin");
        s.poolMax   = pr.getInt("poolMax");
        s.poolIncre = pr.getInt("poolIncre");
        s.idleMax   = pr.getInt("idleMax");
        return s;

      } catch (Exception e) {
        logger.error("Db.Server.valueOf fail", e);
        return null;
      }
    }
    
    public  static final String ReplicaMaster  = "master";
    public  static final String ReplicaSlave   = "slave";
    public  static final int    ModTypeModify = 1;
    public  static final int    ModTypeDelete = 2;
    private static final int    PoolMin       = 1;
    private static final int    PoolMax
        = Math.max(4, Runtime.getRuntime().availableProcessors() * 16);
    private static final int    PoolIncre     = 1;
    private static final int    IdleMax       = 10 * 60;
    
    /** mysql, postgresql, ... */
    public  String  driver;
    public  String  host;
    public  Integer port;
    public  String  database;
    public  String  user;
    public  String  password;
    public  String  replica;
    public  Integer poolMin;
    public  Integer poolMax;
    public  Integer poolIncre;
    public  Integer idleMax;
    
    public  int     modType;
    
    public  ComboPooledDataSource pool;
    
    public  boolean sameUrl(Server s2) {
      return host.equals(s2.host) &&
          port == s2.port &&
          database.equals(s2.database);
    }
    
    public  void resetPool(Server conf) {
      try {
        logger.info("Db.Server.resetPool, {}", this);
        
        replica = v(replica, ReplicaMaster);

        String driver2 = null;
        String url    = null;
        if (driver == null || driver.equals("mysql")) {
            driver2 = "com.mysql.cj.jdbc.Driver";
            url = "jdbc:mysql://" + host + ":" + port + "/" + database
                + "?useUnicode=true&characterEncoding=utf-8";//&useSSL=false
        } else if (driver.equals("postgresql")) {
            driver2 = "org.postgresql.Driver";
            url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        } else {
            logger.error("Db.Server.resetPool.unsupported_driver, {}", driver);
            return;
        }

        int poolMin   = v(this.poolMin  , conf.poolMin  , PoolMin  );
        int poolMax   = v(this.poolMax  , conf.poolMax  , PoolMax  );
        int poolIncre = v(this.poolIncre, conf.poolIncre, PoolIncre);
        int idleMax   = v(this.idleMax  , conf.idleMax  , IdleMax  );
        
        ComboPooledDataSource pool2 = new ComboPooledDataSource();
        pool2.setDriverClass(driver2);
        pool2.setJdbcUrl(url);
        pool2.setUser(user);
        pool2.setPassword(password);
        pool2.setInitialPoolSize(poolMin);
        pool2.setMinPoolSize(poolMin);
        pool2.setMaxPoolSize(poolMax);
        pool2.setAcquireIncrement(poolIncre);
        pool2.setIdleConnectionTestPeriod(idleMax);
        pool2.setMaxIdleTime(idleMax);
        
        ComboPooledDataSource pool1 = pool;
        pool = pool2;
        if (pool1 != null) {
          pool1.close();
        }
        logger.info("Db.Server.resetPool.succ, {}", pool);

      } catch (Exception e) {
        logger.error("Db.Server.resetPool.fail", e);
      }
    }

    public  boolean replicaIsMaster() {
      return replica == null || replica.equals(ReplicaMaster);
    }

    @SuppressWarnings("unused")
    public  boolean replicaIsSlave() {
      return ReplicaSlave.equals(replica);
    }
    
    public  String toString() {
      return "{driver:" + driver
          + ", host:" + host
          + ", port:" + port
          + ", database:" + database
          + ", user:****"
          + ", password:****"
          + ", replica:" + replica
          + ", poolMin:" + poolMin
          + ", poolMax:" + poolMax
          + ", poolIncre:" + poolIncre
          + ", idleMax:" + idleMax
          + ", modType:" + modType
          + "}";
    }
    
  }
  
  private static class ServerGroup {
    public  List<Server>  servers = new ArrayList<Server>();
    public  List<Server>  masters = new ArrayList<Server>();
  }
  
  private        class ConfWatcher implements Conf.Watcher {

    public void changed(List<Conf.Event> events) {
      if (events == null || events.isEmpty()) {return;}

      Conf.Kv kv = events.get(0).kv;
      if (kv == null || kv.value == null) {return;}
      Server server = Server.valueOf(kv.value);
      if (server == null) {return;}
      
      confUpdate(server);
    }

  }
  
  private        class ServersWatcher implements Conf.Watcher {

    public void changed(List<Conf.Event> events) {
      if (events == null || events.isEmpty()) {return;}
      
      List<Server> ss = new ArrayList<Server>(events.size());
      for (Conf.Event e : events) {
        Server s = null;

        if (e.type == Conf.Event.TypeModify) {
          s = Server.valueOf(e.kv.value);
          if (s == null) {continue;}
          s.modType = Server.ModTypeModify;
          
        } else if (e.type == Conf.Event.TypeDelete) {
          s = Server.valueOf(e.preKv.value);
          if (s == null) {continue;}
          s.modType = Server.ModTypeDelete;

        } else {
          continue;
        }
        
        ss.add(s);
      }
      
      serversUpdate(ss);
    }

  }



  private static final Logger logger = LoggerFactory.getLogger(Db.class);

  private Server        conf;
  private ServerGroup   serverGroup = new ServerGroup();
  private AtomicInteger serverIndex = new AtomicInteger();
  private AtomicInteger masterIndex = new AtomicInteger();

  private Db(String name) {
    String confKeyPrefix = Conf.get(ConfKeyPrefix, "");
    String confKeyConf = confKeyPrefix + "db/" + name + "/conf";
    String confKeyServersPrefix = confKeyPrefix + "db/" + name + "/servers/";
    String confKeyServersStart  = Conf.keyStart(confKeyServersPrefix);
    String confKeyServersEnd    = Conf.keyEnd  (confKeyServersPrefix);
    
    Conf.watch(confKeyConf, null, new ConfWatcher());
    confUpdate(Server.valueOf(Conf.get(confKeyConf)));
    
    Conf.watch(confKeyServersStart, confKeyServersEnd, new ServersWatcher());
    List<Conf.Kv> kvs = Conf.range(confKeyServersStart, confKeyServersEnd);
    List<Server>  ss  = new ArrayList<Server>(kvs.size());
    for (Conf.Kv kv : kvs) {
      Server s = Server.valueOf(kv.value);
      if (s == null) {continue;}
      s.modType = Server.ModTypeModify;
      ss.add(s);
    }
    serversUpdate(ss);
  }
  
  
  private synchronized void confUpdate(Server conf) {
    this.conf = conf = v(conf, new Server());
    
    for (Server s : serverGroup.servers) {
      s.resetPool(conf);
    }
  }
  
  private synchronized void serversUpdate(List<Server> ss) {
    if (ss == null || ss.isEmpty()) {return;}
    
    //put {ss} to {this.serverGroup.servers}
    //add none-exist ones, update exist ones, remove delete ones
    List<Server> ss2 = new ArrayList<Server>(this.serverGroup.servers);
    for (Server s : ss) {
      for (int i = ss2.size() - 1; i >= 0; i--) {
        if (s.sameUrl(ss2.get(i))) {
          ss2.remove(i);
          break;
        }
      }
      if (s.modType != Server.ModTypeDelete) {
        ss2.add(s);
        s.resetPool(conf);
      }
    }

    ServerGroup sg2 = new ServerGroup();
    sg2.servers = ss2;
    for (Server s : sg2.servers) {
      if (s.replicaIsMaster()) {
        sg2.masters.add(s);
      }
    }

    //change servers
    this.serverGroup = sg2;
  }
  
  private Server server(boolean useMaster) {
    ServerGroup sg = serverGroup;
    List<Server>  ss    = useMaster ? sg.masters  : sg.servers ;
    AtomicInteger index = useMaster ? masterIndex : serverIndex;
    
    if (ss.isEmpty()  ) {return null;}
    if (ss.size() == 1) {return ss.get(0);}
    return ss.get(index.getAndIncrement() % ss.size());
  }
















  //Part 3.1: database object-relation-mapping

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public  static @interface OrmIgnore {}
  
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public  static @interface OrmClass {
    String table() default "";
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public  static @interface OrmField {
    String  column   () default "";
    boolean id       () default false;
    boolean autoIncre() default false;
  }
  
  public  static void  ormConf(
      Class<?> claz, String table, Map<String, String>  fieldColumnMapping) {
    Ctms.put(claz, Ctm.ctm(claz, table, fieldColumnMapping));
  }
  
  /** Field Column Mapping */
  private static class Fcm {
    private String  fname;
    private String  cname;
    private Field   f;
    @SuppressWarnings("rawtypes")
    private Class   fclass;
    private boolean fclassNumber;
    private Method  fgetter;
    private Method  fsetter;
    private boolean autoIncre;
    private boolean id;
    
    private Fcm(String fname, String cname) {
      this.fname = fname ;
      this.cname = cname;
    }
    
    @SuppressWarnings("rawtypes")
    private void    fclass(Class fclass) {
      this.fclass = fclass;
      this.fclassNumber = Number.class.isAssignableFrom(fclass);
    }
    
    public  boolean fgetable() {
      return fgetter != null || f != null;
    }
    
    public  Object  fget(Object o) throws DbException {
      try {
        if (fgetter != null) {
          return fgetter.invoke(o);
        } else if (f != null) {
          return f.get(o);
        } else {
          throw new DbException("ungetable " + fname);
        }
      } catch (Exception e) {
        throw new DbException("fcm.fget fail," + fname, e);
      }
    }
    
    @SuppressWarnings("unused")
    public  boolean fsetable() {
      return fsetter != null || f != null;
    }
    
    public  void    fset(Object o, Object v) throws DbException {
      try {
        v = numFix(v);
        if (fsetter != null) {
          fsetter.invoke(o, v);
        } else if (f != null) {
          f.set(o, v);
        }
      } catch (Exception e) {
        throw new DbException("fcm.fset fail," + fname , e);
      }
    }
    
    @SuppressWarnings("rawtypes")
    private Object numFix(Object v) {
      if (v == null) {return null;}
      if (!fclassNumber) {return v;}
      
      Class vclass = v.getClass();
      if (vclass == fclass) {return v;}

      if (Number.class.isAssignableFrom(vclass)) {
        Number vn = Number.class.cast(v);
        if (fclass == Byte   .class) {return vn.byteValue ();}
        if (fclass == Short  .class) {return vn.shortValue();}
        if (fclass == Integer.class) {return vn.intValue();}
        if (fclass == Long   .class) {return vn.longValue();}
        if (fclass == BigInteger.class) {
          return BigInteger.valueOf(vn.longValue());
        }
        if (fclass == BigDecimal.class) {
          return BigDecimal.valueOf(vn.doubleValue());
        }
      }
      
      return v;
    }
  }
  
  /** Class Table Mapping */
  private static class Ctm {
    private Class<?>  c;
    private String    t;
    private List<Fcm> fcms = new ArrayList<Fcm>();
    private Map<String, Fcm> fcmsByFname = new HashMap<String, Fcm>();
    private Map<String, Fcm> fcmsByCname = new HashMap<String, Fcm>();
    
    private Ctm(Class<?> c, String t) {
      this.c = c;
      this.t = t;
      
      if (t == null && c != null) {
        OrmClass ormClass = c.getAnnotation(OrmClass.class);
        if (ormClass != null) {
          this.t = ormClass.table();
        }
      }
    }
    
    
    private static Ctm ctm(Class<?> c, String t, Map<String, String> fcs1) {
      
      //calculate class table mapping
      //use getter/setter if exists, else use field
      
      Ctm ctm = new Ctm(c, t);
      
      //map fields
      for (Field f : ctm.c.getFields()) {
        Fcm fcm = fcm(ctm, f, fcs1);
        if (fcm == null) {continue;}
        
        f.setAccessible(true);
        fcm.f = f;
        fcm.fclass(f.getType());
      }
      
      //map getters/setters
      for (Method m : ctm.c.getMethods()) {
        Fcm fcm = fcm(ctm, m, fcs1);
        if (fcm == null) {continue;}

        String mname = m.getName();
        if (mname.startsWith("get")) {
          fcm.fgetter = m;
          if (fcm.fclass == null) {fcm.fclass(m.getReturnType());}
        } else if (mname.startsWith("set")) {
          fcm.fsetter = m;
          if (fcm.fclass == null) {fcm.fclass(m.getParameterTypes()[0]);}
        }
      }
      
      return ctm;
    }
    
    private static Fcm fcm(Ctm ctm, Method m, Map<String, String> fcs1) {
      String mname = m.getName();
      if (!methodIsGsetter(mname))  {return null;}
      if ("getClass".equals(mname)) {return null;}
      return fcm(ctm, m, fname(mname), fcs1);
    }
    
    private static Fcm fcm(Ctm ctm, Field f, Map<String, String> fcs1) {
      return fcm(ctm, f, f.getName(), fcs1);
    }
    
    private static <F extends AccessibleObject & Member> Fcm fcm(
        Ctm ctm, F f, String fname, Map<String, String> fcs1) {

      if ((f.getModifiers() & Modifier.STATIC) > 0) {return null;}

      boolean  aOrmIgnore = f.getAnnotation(OrmIgnore.class) != null;
      OrmField aOrmField = f.getAnnotation(OrmField.class);
      String cname = cname(fname, aOrmIgnore, aOrmField, fcs1);
      if (cname == null) {return null;} //null for ignore
      
      Fcm fcm = ctm.fcmsByFname.get(fname);
      if (fcm == null) {
        fcm = new Fcm(fname, cname);
        ctm.fcms.add(fcm);
        ctm.fcmsByFname.put(fcm.fname, fcm);
        ctm.fcmsByCname.put(fcm.cname, fcm);
      }
      if (aOrmField != null) {
        fcm.id        |= aOrmField.id();
        fcm.autoIncre |= aOrmField.autoIncre();
      }
      return fcm;
    }
    
    private static boolean methodIsGsetter(String mname) {
      return
          mname.length() > 3
          && (mname.startsWith("get") || mname.startsWith("set"));
    }

    private static String fname(String gsetterName) {
      char char0 = gsetterName.charAt(3);
      if (char0 >= 'A' && char0 <= 'Z') {
          char0 += ('a' - 'A');
      }
      return char0 + gsetterName.substring(4);
    }
    
    private static String cname(
        String fname, boolean aOrmIgnore, OrmField aOrmField,
        Map<String, String> fcs1) {
      
      //value: null for ignore; "" for default translate;

      String cname = "";
      if (fcs1 != null && fcs1.containsKey(fname)) {
        cname = fcs1.get(fname);
      }
      if (cname == null || !cname.isEmpty()) {
        return cname;
      }

      if (aOrmIgnore) {return null;}
      if (aOrmField != null) {
        cname = aOrmField.column();
      }
      if (cname == null || !cname.isEmpty()) {
        return cname;
      }
      
      return cname(fname);
    }
    
    private static String cname(String fname) {
      final int diff = 'A' - 'a';
      
      StringBuilder buf = new StringBuilder(fname.length() + 2);
      for (int i = 0; i < fname.length(); i++) {
        char c = fname.charAt(i);
        if (c >= 'A' && c <= 'Z') {
          if (i > 0) {buf.append("_");} //no "_" for 1st char
          c -= diff;
        }
        buf.append(c);
      }
      
      return buf.toString();
    }

  }
  
  private static final ConcurrentHashMap<Class<?>, Ctm> Ctms
      = new ConcurrentHashMap<Class<?>, Db.Ctm>();

  private static Ctm   ctmGet(Class<?> claz) {
    Ctm ctm = Ctms.get(claz);
    if (ctm != null) {return ctm;}
    
    ctm = Ctm.ctm(claz, null, null);
    Ctm ctm2 = Ctms.putIfAbsent(claz, ctm);
    if (ctm2 != null) {ctm = ctm2;}
    return ctm;
  }
  
  
  

  //Part 3.2: database operate
  
  public  static          class DbException extends RuntimeException {

    private static final long serialVersionUID = 2224606427837153733L;
    
    public  DbException() {
      super();
    }
    
    public  DbException(String message) {
      super(message);
    }
    
    public  DbException(Throwable cause) {
      super(cause);
    }

    public  DbException(String message, Throwable cause) {
      super(message, cause);
    }
    
  }
  
  public  static          class SqlException extends DbException {
    
    private static final long serialVersionUID = -6671337928794596561L;
    
    public  SqlException() {
      super();
    }
  
    public  SqlException(Throwable cause) {
      super(cause);
    }
  
  }

  public  static abstract class Op<T extends Op<T>> {

    private Connection conn;
    private boolean    master = true;
    
    public abstract Object op(Connection conn) throws SQLException;
    
    public  Connection conn() {
      return conn;
    }
    
    @SuppressWarnings("unchecked")
    public  T conn(Connection conn) {
      this.conn = conn;
      return (T)this;
    }
    
    public  boolean master() {
      return master;
    }
    
    @SuppressWarnings("unchecked")
    public  T master(boolean master) {
      this.master = master;
      return (T) this;
    }

  }

  /**
   * <pre>
   * How to use:
   *   1 set sql
   *   2 set params if has
   *   3 set ormClass if need ORM, or Map will be used
   * </pre>
   */
  public  static          class Query<O>  extends Op<Query<O>> {
    
    private String    sql;
    private Object[]  params;
    /** (optional) performance is better */
    private int       possibleCount = 0;
    /** type must be specified because of java's Type Erasure */
    private Class<O>  ormClass;

    //for result
    private int       ccount = 0;
    private String[]  cnames;
    private Ctm       ctm;
    private Fcm[]     fcms;

    
    public  Query() {}
    
    public  Query(Class<O> ormClass) {
      this.ormClass = ormClass;
    }

    
    public  String   sql() {
      return sql;
    }
    
    public  Query<O> sql(String sql) {
      this.sql = sql;
      return this;
    }

    public  Object[] params() {
      return params;
    }
    
    public  Query<O> params(Object... params) {
      this.params = params;
      return this;
    }

    public  int      possibleCount() {
      return possibleCount;
    }
    
    public  Query<O> possibleCount(int possibleCount) {
      this.possibleCount = possibleCount;
      return this;
    }

    public  Class<O> ormClass() {
      return ormClass;
    }
    
    public  Query<O> ormClass(Class<O> ormClass) {
      this.ormClass = ormClass;
      return this;
    }
    

    public  Object   op(Connection db) {

      logger.debug("Db.query.sql,{},{}", sql, params);
      PreparedStatement stm = null;

      try {
        stm = db.prepareStatement(sql);
        
        if (params != null) {
          for (int i = 0; i < params.length; i++) {
            stm.setObject(i + 1, params[i]);
          }
        }
        
        ResultSet rs = stm.executeQuery();

        List<O> r = possibleCount > 0
            ? new ArrayList<O>(possibleCount)
            : new ArrayList<O>();
        while (rs.next()) {
          r.add(result(rs));
        }

        logger.debug("Db.query.return,{}", r);
        return r;

      } catch (SQLException e) {
        logger.error("Db.query.fail,{},{}", sql, params);
        throw new SqlException(e);

      } finally {
        close(stm);
      }
    }
    
    @SuppressWarnings("unchecked")
    public  O        result(ResultSet rs) throws SQLException {
      if (ccount == 0) {
        ccount = rs.getMetaData().getColumnCount();
      }
      
      //try orm
      if (initRsCtm(rs)) {
        try {
          Object o = ctm.c.newInstance();
          for (int i = 0; i < ccount; i++) {
            Object v = rs.getObject(i + 1);
            if (v == null) {continue;}
            if (fcms[i] == null) {continue;}
            fcms[i].fset(o, v);
          }
          return (O) o;
          
        } catch (Exception e) {
          throw new RuntimeException("orm fail", e);
        }
      }

      //try map as simple object or map
      if (ccount == 1) {
        return (O) rs.getObject(1);
      } else {
        Map<String, Object> map =
            new HashMap<String, Object>(ccount + ccount / 2);
        for (int i = 0; i < ccount; i++) {
          map.put(cnames[i], rs.getObject(i + 1));
        }
        return (O) map;
      }
    }
    
    private boolean  initRsCtm(ResultSet rs) throws SQLException {
      if (ormClass == null) {return false;}
      if (ctm != null) {return true;}

      ctm    = ctmGet(ormClass);
      cnames = new String[ccount];
      fcms   = new Fcm   [ccount];
      ResultSetMetaData meta = rs.getMetaData();
      for (int i = 0; i < ccount; i++) {
        String cname = meta.getColumnLabel(i + 1).toLowerCase();
        cnames[i] = cname;
        fcms  [i] = ctm.fcmsByCname.get(cname);
      }
      
      return true;
    }
    
  }

  public  static          class Update    extends Op<Update> {

    private String    sql;
    private Object[]  params;
    private boolean   returnGeneratedKeys = false;
    private Object    generatedKeys;
    //for orm
    private Object    ormObj;
    private List<Fcm> fcmAutoIncres;


    public  String   sql() {
      return sql;
    }
    
    public  Update   sql(String sql) {
      this.sql = sql;
      return this;
    }

    public  Object[] params() {
      return params;
    }
    
    public  Update   params(Object... params) {
      this.params = params;
      return this;
    }
    
    public  boolean  returnGeneratedKeys() {
      return returnGeneratedKeys;
    }
    
    public  Update   returnGeneratedKeys(boolean returnGeneratedKeys) {
      this.returnGeneratedKeys = returnGeneratedKeys;
      return this;
    }
    
    public  Object   generatedKeys() {
      return generatedKeys;
    }
    
    
    private Update   ormObj(Object ormObj) {
      this.ormObj = ormObj;
      return this;
    }
    
    private Update   fcmAutoIncres(List<Fcm> fcmAutoIncres) {
      this.fcmAutoIncres = fcmAutoIncres;
      return this;
    }
    
    public  Object   op(Connection db) {
      logger.debug("Db.update.sql,{},{}", sql, params);
      PreparedStatement stm = null;

      try {
        if (returnGeneratedKeys) {
          stm = db.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        } else {
          stm = db.prepareStatement(sql);
        }
        
        if (params != null) {
          for (int i = 0; i < params.length; i++) {
            stm.setObject(i + 1, params[i]);
          }
        }

        int r = stm.executeUpdate();
        if (returnGeneratedKeys) {
          ResultSet rs = stm.getGeneratedKeys();
          if (rs.next()) {
            generatedKeys = generatedKeys(rs);
          }
        }

        if (returnGeneratedKeys) {
          logger.debug("Db.update.return,{},{}", r, generatedKeys);
        } else {
          logger.debug("Db.update.return,{}", r);
        }
        return r;

      } catch (SQLException e) {
        logger.error("Db.update.fail,{},{}", sql, params);
        throw new SqlException(e);

      } finally {
        close(stm);
      }
    }

    private Object   generatedKeys(ResultSet rs)
    throws SQLException {
      
      ResultSetMetaData meta = rs.getMetaData();
      int ccount = meta.getColumnCount();
      
      if (ccount <= 0) {
        return null;
        
      } else if (ccount == 1) {
        Object r = generatedKeyFix(rs.getObject(1));
        if (ormObj !=null && fcmAutoIncres !=null && !fcmAutoIncres.isEmpty()) {
          fcmAutoIncres.get(0).fset(ormObj, r);
        }
        return r;
        
      } else {
        Map<String, Object> map =
            new HashMap<String, Object>(ccount + ccount / 2 + 1);
        for (int i = 1; i <= ccount; i++) {
          map.put(meta.getColumnName(i), generatedKeyFix(rs.getObject(i)));
        }
        return map;
      }
    }
    
    private Object   generatedKeyFix(Object r) {
      //type: BigInteger => Long
      if (BigInteger.class.isInstance(r)) {
        if (((BigInteger)r).bitLength() < 64) {
          r = ((BigInteger)r).longValue();
        }
      }
      return r;
    }

  }

  public  static          class Batch     extends Op<Batch> {
    
    private String         sql;
    private List<Object[]> params;
    private List<String>   sqls;
    
    public  Object op(Connection db) {
      logger.debug("Db.batch");
      Statement         stm = null;

      try {
        if (sql != null) {
          PreparedStatement stm2 = db.prepareStatement(sql);
          stm = stm2;
          for (Object[] p : params) {
            if (p != null) {
              for (int i = 0; i < p.length; i++) {
                stm2.setObject(i + 1, p[i]);
              }
            }
            stm2.addBatch();
          }
        } else {
          stm = db.createStatement();
          for (String sql : sqls) {
            stm.addBatch(sql);
          }
        }

        int[] r = stm.executeBatch();
        
        logger.debug("Db.batch.return,{}", r);
        return r;

      } catch (SQLException e) {
        throw new SqlException(e);

      } finally {
        close(stm);
      }
    }

    public  String         sql() {
      return sql;
    }
    
    public  Batch          sql(String sql) {
      this.sql = sql;
      return this;
    }

    public  List<Object[]> params() {
      return params;
    }
    
    public  Batch          params(List<Object[]> params) {
      this.params = params;
      return this;
    }

    public  List<String>   sqls() {
      return sqls;
    }
    
    public  Batch          sqls(List<String> sqls) {
      this.sqls = sqls;
      return this;
    }

  }


  /** MUST CLOSE CONNECTION AFTER USING !!! */
  public  Connection  getConnection() throws SqlException {
    try {
      return getConnection(null);
    } catch (SQLException e) {
      throw new SqlException(e);
    }
  }

  public  Object      op(Op<?> op) throws SqlException {
    Connection        db = null;
    try {
      db = getConnection(op);
      return op.op(db);
    } catch (SQLException e) {
      throw new SqlException(e);
    } finally {
      close(db, op);
    }
  }

  @SuppressWarnings("unchecked")
  public  <O> List<O> query(Query<O> op) throws SqlException {
    return (List<O>) op(op);
  }

  public  <O> O       querySingle(Query<O> op) throws SqlException {
    List<O> r = query(op.possibleCount(1));
    return r.isEmpty() ? null : r.get(0);
  }

  public  int         update(Update op) throws SqlException {
    return ((Integer) op(op)).intValue();
  }

  public  int[]       batch(Batch op) throws SqlException {
    return (int[]) op(op);
  }

  private Connection  getConnection(Op<?> op) throws SQLException {
    if (op != null && op.conn != null) {
      return op.conn;
    }
    return server(op.master).pool.getConnection();
  }

  private static void close(Connection conn, Op<?> op) {
    if (op != null && op.conn == conn) {
      return;
    }
    if (conn != null) {
      try {conn.close();} catch (Exception e) {}
    }
  }

  private static void close(Statement stm) {
    if (stm != null) {
      try {stm.close();} catch (Exception e) {}
    }
  }

  
  //ORM operate

  public  static      class OpParam {
    private String   table  = null;
    private boolean  ignore = false;
    private Object[] where  = null;
    private String[] order  = null;
    private int[]    limit  = null;
    
    public  OpParam table(String table) {
      this.table = table;
      return this;
    }
    public  OpParam ignore(boolean ignore) {
      this.ignore = ignore;
      return this;
    }
    public  OpParam where(Object... where) {
      this.where = where;
      return this;
    }
    public  OpParam order(String... order) {
      this.order = order;
      return this;
    }
    public  OpParam limit(int... limit) {
      this.limit = limit;
      return this;
    }
  }

  public  <O> O       get(O id) {
    @SuppressWarnings("unchecked")
    Class<O> claz = (Class<O>) id.getClass();
    Ctm ctm = ctmGet(claz);
    
    StringBuilder sql    = new StringBuilder();
    List<Object>  params = new ArrayList<Object>(2);
    boolean       first  = true;
    sql.append("select * from ").append(ctm.t);
    for (Fcm fcm : ctm.fcms) {
      if (!fcm.id || !fcm.fgetable()) {continue;}
      sql.append(first ? " where " : " and ").append(fcm.cname).append("=?");
      params.add(fcm.fget(id));
      first = false;
    }
    if (params.isEmpty()) {throw new DbException("no id");}
    
    return querySingle(new Query<O>(claz)
        .sql(sql.toString())
        .params(params.toArray()));
  }

  public  <O> List<O> list(Class<O> claz) {
    return list(claz, null);
  }

  public  <O> List<O> list(Class<O> claz, OpParam p) {
    Ctm ctm = ctmGet(claz);
    
    StringBuilder sql    = new StringBuilder();
    List<Object>  params = new ArrayList<Object>(2);
    
    sql.append("select * from");
    table(p, sql, ctm);
    where(p, sql, params, ctm);
    order(p, sql, ctm);
    limit(p, sql, params);
    
    return query(new Query<O>(claz)
        .sql(sql.toString())
        .params(params.toArray()));
  }
  
  public  int         add(Object o) {
    return add(o, null);
  }
  
  public  int         add(Object o, OpParam p) {
    if (o == null) {return 0;}
    
    Ctm ctm = ctmGet(o.getClass());
    StringBuilder sql = new StringBuilder();
    StringBuilder qms = new StringBuilder();
    List<Object>  params = new ArrayList<Object>();
    List<Fcm>     autoIncres = new ArrayList<Fcm>();
    
    sql.append("insert")
      .append(p != null && p.ignore ? " ignore into " : " into ")
      .append(p != null && p.table != null ? p.table : ctm.t)
      .append("(");
    boolean first = true;
    for (Fcm fcm : ctm.fcms) {
      if (!fcm.fgetable()) {continue;}
      
      Object v = fcm.fget(o);
      if (fcm.autoIncre && v == null) {
        autoIncres.add(fcm);
        continue;
      }
      params.add(v);
      
      sql.append(first ? "" : ",").append(fcm.cname);
      qms.append(first ? "" : ",").append("?");
      first = false;
    }
    sql.append(") value(").append(qms.toString()).append(")");
    
    return update(new Update()
        .sql(sql.toString())
        .params(params.toArray())
        .returnGeneratedKeys(!autoIncres.isEmpty())
        .fcmAutoIncres(autoIncres)
        .ormObj(o)
        );
  }
  
  /** modify object's not-null fields by id */
  public  int         mod(Object o) {
    Object[] whereIds = whereIds(o);
    if (whereIds.length == 0) {throw new DbException("No id");}
    return mod(o, new OpParam().where(whereIds));
  }

  /**
   * modify object's not-null fields by where<br/>
   * <pre>
   * eg:
   * mod(user, "id", 15L, "version", 12)
   * //update user ... where id=15 and version=12
   * </pre>
   */
  public  int         mod(Object o, OpParam p) {
    Ctm ctm = ctmGet(o.getClass());
    StringBuilder sql = new StringBuilder();
    List<Object>  params = new ArrayList<Object>();
    
    //update tablex
    sql.append("update");
    table(p, sql, ctm);
    
    //set xx=?,xy=?
    boolean first = true;
    for (Fcm fcm : ctm.fcms) {
      if (!fcm.fgetable()) {continue;}
      
      Object v = fcm.fget(o);
      if (v == null) {continue;}
      
      sql.append(first ? " set " : ",").append(fcm.cname).append("=?");
      params.add(v);
      first = false;
    }
    if (params.isEmpty()) {
      throw new DbException("No fields to mod");
    }
    
    where(p, sql, params, ctm);
    
    return update(new Update()
        .sql(sql.toString())
        .params(params.toArray()));
  }

  /** delete object by id */
  public  int         del(Object o) {
    Object[] whereIds = whereIds(o);
    if (whereIds.length == 0) {throw new DbException("No id");}
    return del(o, new OpParam().where(whereIds));
  }
  
  /** delete object by where */
  public  int         del(Object o, OpParam p) {
    Ctm ctm = ctmGet(o.getClass());
    StringBuilder sql = new StringBuilder();
    List<Object>  params = new ArrayList<Object>();
    
    sql.append("delete from");
    table(p, sql, ctm);
    where(p, sql, params, ctm);
    limit(p, sql, params);
    
    return update(new Update()
        .sql(sql.toString())
        .params(params.toArray()));
  
  }

  private Object[]    whereIds(Object o) {
    Ctm ctm = ctmGet(o.getClass());
    ArrayList<Object> where = new ArrayList<Object>();
    
    for (Fcm fcm : ctm.fcms) {
      if (fcm.id && fcm.fgetable()) {
        where.add(fcm.fname);
        where.add(fcm.fget(o));
      }
    }
    
    return where.toArray();
  }
  
  private void        table(OpParam p, StringBuilder sql, Ctm ctm) {
    sql.append(" ").append(p != null && p.table != null ? p.table : ctm.t);
  }
  
  private void        where(
      OpParam p, StringBuilder sql, List<Object> params, Ctm ctm) {
    //where aa=? and ab=?
    if (p != null && p.where != null && p.where.length > 0) {
      boolean first = true;
      for (int i = 0; i < p.where.length; i+=2) {
        String fname = p.where[i].toString();
        Fcm w = ctm.fcmsByFname.get(fname);
        if (w == null) {throw new DbException("no field " + fname);}
        
        sql.append(first ? " where " : " and ").append(w.cname).append("=?");
        params.add(p.where[i + 1]);
        first = false;
      }
    }
  }
  
  private void        order(OpParam p, StringBuilder sql, Ctm ctm) {
    if (p != null && p.order != null && p.order.length > 0) {
      boolean first = true;
      for (int i = 0; i < p.order.length; i++) {
        String fname = p.order[i].toString();
        Fcm fcm = ctm.fcmsByFname.get(fname);
        if (fcm == null) {throw new DbException("no field " + fname);}
        
        sql.append(first ? " order by " : ",").append(fcm.cname);
      }
    }
  }
  
  private void        limit(OpParam p, StringBuilder sql, List<Object> params) {
    //limit ?,?
    if (p != null && p.limit != null && p.limit.length > 0) {
      sql.append(" limit ").append(p.limit.length == 1 ? "?" : "?,?");
      params.add(p.limit[0]);
      if (p.limit.length > 1) {
        params.add(p.limit[1]);
      }
    }
  }
  















  //Part 4: utility
  
  private static class PropertyReader {
    
    private Properties p;
    
    public PropertyReader(String s) throws Exception {
      p = new Properties();
      p.load(new StringReader(s));
    }
    
    public Integer getInt(String name) {
      return getInt(name, null);
    }
    
    public Integer getInt(String name, Integer dft) {
      return str2int(p.getProperty(name), dft);
    }
    
    public String get(String name) {
      return p.getProperty(name);
    }
    
  }
  
  private static Integer str2int(String str, Integer dft) {
    try {
      return Integer.parseInt(str);
    } catch (NumberFormatException e) {
      return dft;
    }
  }
  
  private static <T> T   v(T... vs) {
    for (T v : vs) {
      if (v != null) {return v;}
    }
    return null;
  }
  
}
