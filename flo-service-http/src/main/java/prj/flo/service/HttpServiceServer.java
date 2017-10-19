package prj.flo.service;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.ServerSocket;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpPostMultipartRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;
import prj.flo.conf.Conf;
import prj.flo.service.HttpService.HttpConf;
import prj.flo.service.HttpService.HttpDataFile;
import prj.flo.service.HttpService.HttpReq;
import prj.flo.service.Service.ServiceException;

/**
 * HTTP service server.
 * 
 * @author Leon Dong
 */
public  class HttpServiceServer implements ServiceServer {
  
  private static final Logger logger = LoggerFactory.getLogger(
      HttpServiceServer.class);
  
  private static final Charset Utf8 = Charset.forName("UTF-8");
  
  private static final String  ConfKeyPrefix = "flo_service_conf_key_prefix";
  
  
  
  
  //part 1: service server container.
  //contain service server, manage port, dispatch req
  
  //part 1.1: contain service server
  
  private static HttpServiceServer Container = new HttpServiceServer();
  
  public  static HttpServiceServer server() {
    return Container;
  }

  
  private ArrayList<HttpServiceServer> servers;
  
  private HttpServiceServer() {
    this.servers = new ArrayList<HttpServiceServer>();

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      public void run() {stopAll();}
    }, "HttpServiceServer.stopAll"));
  }
  
  
  public  <I> void start(I service, Class<I> itface) {
    if (service == null) {throw new NullPointerException("service is null");}
    if (itface  == null) {throw new NullPointerException("itface is null");}
    
    HttpServiceServer server = serverOrNew(service, itface);
    server.start();
  }
  
  public  <I> void start(I service) {
    start(service, itface(service));
  }
  
  public      void stop (String name) {
    if (name == null) {
      logger.info("no service null");
      return;
    }

    HttpServiceServer server = serverByName(name);
    if (server == null) {
      logger.info("no service {}", name);
      return;
    }
    
    server.stop();
  }
  
  private     void          stopAll() {
    for (HttpServiceServer s : servers) {
      s.stop();
    }
  }
  
  @SuppressWarnings("unchecked")
  private <I> Class<I>      itface(I service) {
    if (service == null) {throw new NullPointerException("service is null");}
    
    Class<?>[] itfaces = service.getClass().getInterfaces();
    if (itfaces.length != 1) {
      throw new ServiceException(
          "service must has one interface if not specify one");
    }
    
    return (Class<I>) itfaces[0];
  }
  
  private HttpServiceServer serverByName(String name) {
    for (int n = servers.size() - 1; n >= 0; n--) {
      HttpServiceServer hss = servers.get(n);
      if (hss.name.equals(name)) {
        return hss;
      }
    }
    return null;
  }
  
  private HttpServiceServer serverOrNew(Object s, Class<?> i) {
    HttpServiceServer server = serverByItface(i);
    if (server != null) {return server;}
    
    synchronized(servers) {
      server = serverByItface(i);
      if (server != null) {return server;}

      server = new HttpServiceServer(this, s, i);
      servers.add(server);
    }
    
    return server;
  }
  
  private HttpServiceServer serverByItface(Class<?> i) {
    for (int n = servers.size() - 1; n >= 0; n--) {
      HttpServiceServer hss = servers.get(n);
      if (hss.i == i) {
        return hss;
      }
    }
    return null;
  }
  
  
  
  
  //part 1.2: manage port, dispatch req
  
  private static class DispatchMethod {
    public  Method m;
    public  Parameter[] ps;
  }
  
  private static class Dispatcher extends ChannelInboundHandlerAdapter {
    
    private HttpServiceServer container;
    
    public  Dispatcher(HttpServiceServer container) {
      this.container = container;
    }
    
    
    //start, stop, http message read
    
    private EventLoopGroup  bossGroup;
    private EventLoopGroup  workerGroup;
    
    public  void start(int port, int contentLengthMax) {
      try {
        final int contentLengthMax2 = contentLengthMax > 0
            ? contentLengthMax : 1 * 1024 * 1024;
        
        bossGroup   = new NioEventLoopGroup( 8, new ThreadFactory() {
          private AtomicInteger id = new AtomicInteger();
          public Thread newThread(Runnable r) {
            return new Thread(r, "hss_nio_" + id.incrementAndGet());
          }
        });
        workerGroup = new NioEventLoopGroup(16, new ThreadFactory() {
          private AtomicInteger id = new AtomicInteger();
          public Thread newThread(Runnable r) {
            return new Thread(r, "hss_worker_" + id.incrementAndGet());
          }
        });
        final ChannelInboundHandlerAdapter handler = this;
        
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .childHandler(new ChannelInitializer<SocketChannel>() {
            protected void initChannel(SocketChannel ch) throws Exception {
              ch.pipeline()
                .addLast("decoder", new HttpRequestDecoder())
                .addLast("encoder", new HttpResponseEncoder())
                .addLast("agregtr", new HttpObjectAggregator(contentLengthMax2))
                .addLast("handler", handler);
          }
        });
        
        b.bind(port).sync();
        
      } catch (Exception e) {
        throw new ServiceException("Dispatcher.start.fail",e);
      }
    }

    public  void stop() {
      try {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        
      } catch (Exception e) {
        logger.error("Dispatcher.stop.fail", e);
        
      } finally {
        bossGroup  = null;
        workerGroup = null;
      }
    }
    
    public  boolean isSharable() {
      return true;
    }
    
    public  void channelRead(ChannelHandlerContext ctx, Object msg)
    throws Exception {
      if (msg instanceof FullHttpRequest) {
        FullHttpRequest msg1 = (FullHttpRequest) msg;
        HttpReq req = new HttpReq(ctx);
        
        //headers
        req.isKeepAlive = HttpUtil.isKeepAlive(msg1);
        
        //method
        req.method = msg1.method().name();
        
        //uri
        req.uri = msg1.uri();
        int iUriQues = req.uri.indexOf("?");
        if (iUriQues >= 0) {
          req.uri = req.uri.substring(0, iUriQues);
        }
        
        //headers
        for (Map.Entry<String,String> e : msg1.headers().entries()) {
          if (!req.headers.containsKey(e.getKey())) {
            req.headers.put(e.getKey(), e.getValue());
          }
        }
        
        //params in URL
        if (iUriQues >= 0) {
          decodeParam(msg1.uri().substring(iUriQues + 1), req.params);
        }
        
        //params by HTTP POST
        if (req.method.equalsIgnoreCase("POST")) {
          String contentType = msg1.headers().get(HttpHeaderNames.CONTENT_TYPE);
          if (contentType == null ||
              !contentType.toLowerCase().startsWith("multipart/form-data")) {
            
            //params by normal HTTP POST
            decodeParam(msg1.content().toString(Utf8), req.params);
            
          } else {
            //params by POST multipart/form-data
            HttpPostMultipartRequestDecoder decoder =
                new HttpPostMultipartRequestDecoder(
                    new DefaultHttpDataFactory(false), msg1);
            for (InterfaceHttpData data : decoder.getBodyHttpDatas()) {
              if (data.getHttpDataType() == HttpDataType.FileUpload) {
                FileUpload f = (FileUpload) data;
                int    flen = (int) f.length();
                byte[] fcontent;
                ByteBuf fbyte = f.getByteBuf();
                if (fbyte.hasArray() && fbyte.arrayOffset() == 0 &&
                    fbyte.array().length == flen) {
                  fcontent = fbyte.array();
                } else {
                  fcontent = new byte[flen];
                  fbyte.readerIndex(0);
                  fbyte.readBytes(fcontent, 0, flen);
                }
                req.files.put(
                    URLDecoder.decode(f.getName(), "UTF-8"),
                    new HttpDataFile(
                        f.getFilename(), f.getContentType(), fcontent));
              } else {
                Attribute data2 = (Attribute) data;
                req.params.put(
                    URLDecoder.decode(data2.getName(), "UTF-8"),
                    data2.content().toString(Utf8));
              }
            }
            decoder.destroy();
          }
        }
        //msg1.content().release(); //realease content CompositeByteBuf

        dispatch(req);
      }
    }

    public  void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      logger.error("Dispatcher.read.fail", cause);
      ctx.close();
    }

    private void decodeParam(String raw, Map<String,String> map) {
      if (raw == null || raw.isEmpty()) {return;}
      
      try {
        String[] raws2 = raw.split("&");
        for (String raw2 : raws2) {
          String[] raws3 = raw2.split("=");
          String k = URLDecoder.decode(raws3[0], "UTF-8");
          String v = raws3.length > 1 ? URLDecoder.decode(raws3[1], "UTF-8") : "";
          if (!map.containsKey(k)) {
            map.put(k, v);
          }
        }
        
      } catch (Exception e) {
        logger.warn("Dispatcher.decodeParam.fail", e);
      }
    }
    
    
    //http req dispatch
    
    private CopyOnWriteArrayList<HttpServiceServer> servers
      = new CopyOnWriteArrayList<HttpServiceServer>();
    
    public  void add(HttpServiceServer server) {
      servers.addIfAbsent(server);
    }
    
    public  void rm (HttpServiceServer server) {
      servers.remove(server);
      
      if (servers.isEmpty()) {
        stop();
        container.dispatchers.remove(this);
      }
    }
    
    private void dispatch(HttpReq req) {
      boolean dispatched = false;
      Object  r = null;
      
      try {
        for (HttpServiceServer s : servers) {
          if (s.dispatchable(req)) {
            dispatched = true;
            r = s.dispatch(req);
            break;
          }
        }
        
      } catch (Exception e) {
        logger.error("Dispatcher.dispatch.fail", e);
        req.resp().status(502).flush();
        return;
      }
      if (!dispatched) {
        req.resp().status(404).flush();
        return;
      }
      
      if (byte[].class.isInstance(r)) {
        req.resp().content((byte[])r).flush();
        return;
      }
      
      if (r != null) {
        if (!(r instanceof String)) {
          r = json(r);
        }
        req.resp().content((String) r);
      }
      req.resp().flush();
    }
    
  }
  
  private ConcurrentHashMap<Integer, Dispatcher> dispatchers
    = new ConcurrentHashMap<Integer, Dispatcher>();
  
  private Dispatcher dispatcher(int port, int contentLengthMax) {
    Dispatcher d = dispatchers.get(port);
    if (d != null) {return d;}
    
    synchronized(dispatchers) {
      d = dispatchers.get(port);
      if (d != null) {return d;}
      
      d = new Dispatcher(this);
      d.start(port, contentLengthMax);
      dispatchers.put(port, d);
    }
    
    return d;
  }
  

  
  
  
  
  
  
  
  
  
  
  

  
  
  //part 2: service server (that serves one service)
  //func:
  //  1 reg service to conf
  //  2 watch conf for service control
  
  private static final int StatusStoped   = 0;
  private static final int StatusStarting = 1;
  private static final int StatusStarted  = 2;
  private static final int StatusStoping  = 3;

  private AtomicInteger     status;
  private HttpServiceServer container;
  private Object            s; //service
  private Class<?>          i; //interface
  private String            name;
  private HttpConf          conf;
  private TreeMap<String, DispatchMethod> dms; //dispatchMethods
  
  private HttpServiceServer(HttpServiceServer container, Object s, Class<?> i) {
    this.container = container;
    this.s = s;
    this.i = i;
    
    status = new AtomicInteger(StatusStoped);
    name   = i.getCanonicalName();

    
    //conf
    conf = new HttpConf();
    String confKeyPrefix = Conf.get(HttpServiceServer.ConfKeyPrefix, "");
    PropertyReader pr = new PropertyReader(Conf.get(
        confKeyPrefix + "service/" + name + "/conf/base", ""));
    
    conf.logReq    = v(i2b(pr.getInt("http_log_req")), conf.logReq);
    conf.port      = pr.getInt("http_port", 0);
    if (conf.port == 0) {conf.port = portUsable();}
    conf.contentLengthMax = v(
        pr.getInt("http_content_length_max"), conf.contentLengthMax);
    conf.cors      = pr.get("http_cors");
    conf.uriPrefix = v(pr.get("http_uri_prefix"), conf.uriPrefix);
    
    
    //dmap
    dms = new TreeMap<String, DispatchMethod>();
    for (Method m : i.getMethods()) {
      String uri = conf.uriPrefix + m.getName();
      DispatchMethod dm = new DispatchMethod();
      dms.put(uri, dm);
      dm.m = m;
      dm.ps = m.getParameters();
    }
  }
  
  private void start() {
    if (!status.compareAndSet(StatusStoped, StatusStarting)) {
      return;
    }
    
    try {
      logger.info("starting service " + name);
      container.dispatcher(conf.port, conf.contentLengthMax).add(this);
      
      status.set(StatusStarted);
      logger.info("started service " + name);
      
    } catch (Exception e) {
      status.set(StatusStoped);
      logger.error("HttpServiceServer.start.fail", e);
    }
  }
  
  private void stop() {
    if (!status.compareAndSet(StatusStarted, StatusStoping)) {
      return;
    }
    
    try {
      logger.info("stoping service " + name);
      container.dispatcher(conf.port, conf.contentLengthMax).rm(this);
      
    } catch (Exception e) {
      logger.error("HttpServiceServer.stop.fail", e);
      
    } finally {
      status.set(StatusStoped);
      logger.info("stoped service " + name);
    }
  }
  

  private boolean dispatchable(HttpReq req) {
    return dms.containsKey(req.uri());
  }
  
  @SuppressWarnings("rawtypes")
  private Object  dispatch(HttpReq req) throws Exception {
    if (conf.logReq) {
      logger.info("httpReq: {}, {}", req.uri, req.params);
    }
    
    DispatchMethod dm = dms.get(req.uri());
    List<Object> params = new ArrayList<Object>();
    for (Parameter p : dm.ps) {
      params.add(json(req.params.get(p.getName()), p.getType()));
    }

    Object r = dm.m.invoke(s, params.toArray());
    if (Future.class.isInstance(r)) {
      r = ((Future)r).get();
    }
    
    return r;
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  

  
  //part 3: utility
  
  private static class PropertyReader {
    
    private Properties p;
    
    public PropertyReader(String s) {
      p = new Properties();
      try {
        p.load(new StringReader(s));
      } catch (IOException e) {}
    }
    
    public Integer getInt(String name) {
      return getInt(name, null);
    }
    
    public Integer getInt(String name, Integer dft) {
      return s2i(p.getProperty(name), dft);
    }
    
    public String get(String name) {
      return p.getProperty(name);
    }
    
  }
  
  private static Integer s2i(String str, Integer dft) {
    try {
      return Integer.parseInt(str);
    } catch (NumberFormatException e) {
      return dft;
    }
  }
  
  private static Boolean i2b(Integer i) {
    if (i == null) {return null;}
    return i != 0;
  }
  
  @SuppressWarnings("unchecked")
  private static <T> T   v(T... vs) {
    for (T v : vs) {
      if (v != null) {return v;}
    }
    return null;
  }
  
  private static int     portUsable() {
    try {
        ServerSocket serverSocket = new ServerSocket(0);
        int port = serverSocket.getLocalPort();
        serverSocket.close();
        return port;
    } catch (IOException e) {
        return 0;
    }
  }
  
  private static final ObjectMapper Om = new ObjectMapper();
  static {
    Om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    Om.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  }
  
  @SuppressWarnings("unchecked")
  private static <T> T  json(String json, Class<T> claz) {
    if (json == null) {return null;}
    if (json.isEmpty()) {
      if (String.class.equals(claz)) {return (T) json;}
      return null;
    }
    
    try {
      return Om.readValue(json, claz);
    } catch (Exception e) {
      logger.warn("json.fail", e);
      return null;
    }
  }
  
  private static String json(Object obj) {
    try {
      return Om.writeValueAsString(obj);
    } catch (Exception e) {
      logger.warn("json.fail", e);
      return null;
    }
  }
  

  
}
