package prj.flo.service;

import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpPostMultipartRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType;

/**
 * HTTP service server.
 * 
 * @author Leon Dong
 */
public  class HttpServiceServer {
  
  private static final Logger logger = LoggerFactory.getLogger(
      HttpServiceServer.class);
  
  private static final Charset Utf8 = Charset.forName("UTF-8");
  
  
  
  
  public  static class HttpServiceConf {
    
    private int            port = 8080;
    private HttpReqHandler reqHandler;
    private boolean        logReq = true;
    private String         cors = null;
    private int            contentLengthMax = 10 * 1024 * 1024;
    
    public  HttpServiceConf() {}
    
    public  HttpServiceConf(HttpServiceConf conf1) {
      this.port       = conf1.port;
      this.reqHandler = conf1.reqHandler;
      this.logReq     = conf1.logReq;
      this.cors       = conf1.cors;
      this.contentLengthMax = conf1.contentLengthMax;
    }
    
    public  HttpServiceConf port(int port) {
      this.port = port;
      return this;
    }
    
    public  HttpServiceConf reqHandler(HttpReqHandler reqHandler) {
      this.reqHandler = reqHandler;
      return this;
    }
    
    public  HttpServiceConf logReg(boolean logReq) {
      this.logReq = logReq;
      return this;
    }
    
    public  HttpServiceConf cors(String site) {
      this.cors = site;
      return this;
    }
    
    public  HttpServiceConf contentLengthMax(int contentLengthMax) {
      this.contentLengthMax = contentLengthMax;
      return this;
    }
    
  }

  private static final int StatusStoped   = 0;
  private static final int StatusStarting = 1;
  private static final int StatusStarted  = 2;
  private static final int StatusStoping  = 3;
  
  private HttpServiceConf conf;
  private AtomicInteger   status = new AtomicInteger();
  private EventLoopGroup  bossGroup;
  private EventLoopGroup  workerGroup;
  
  public  HttpServiceServer(HttpServiceConf conf) {
    this.conf = new HttpServiceConf(conf);
    
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      public void run() {stop();}
    }, "HttpServiceServerStop"));
  }
  
  
  
  
  public  void start() {
    if (!status.compareAndSet(StatusStoped, StatusStarting)) {
      return;
    }
    
    try {
      bossGroup = new NioEventLoopGroup(8);
      workerGroup = new NioEventLoopGroup(16);
      
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
       .channel(NioServerSocketChannel.class)
       .childHandler(new ChannelInitializer<SocketChannel>() {
          protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline()
              .addLast("decoder", new HttpRequestDecoder())
              .addLast("encoder", new HttpResponseEncoder())
              .addLast("agregtr", new HttpObjectAggregator(
                  conf.contentLengthMax))
              .addLast("handler", new HttpRequestHandler());
        }
      });
      
      b.bind(conf.port).sync();
      status.set(StatusStarted);
      
    } catch (Exception e) {
      logger.error("HttpServiceServer.start.fail", e);
      status.set(StatusStoped);
    }
  }
  
  public  void stop() {
    if (!status.compareAndSet(StatusStarted, StatusStoping)) {
      return;
    }
    try {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    } catch (Exception e) {
      logger.error("HttpServiceServer.stop.fail", e);
    } finally {
      bossGroup  = null;
      workerGroup = null;
      status.set(StatusStoped);
    }
  }
  
  
  
  
  public  static interface HttpReqHandler {
    
    void req(HttpReq req);
    
  }
  
  public  static class HttpReq {
    
    private HttpServiceConf conf;
    private ChannelHandlerContext ctx;
    
    private String remoteIp;
    private String method;
    private String uri;
    private Map<String,String> headers = new HashMap<String,String>();
    private Map<String,String> params = new HashMap<String,String>();
    private Map<String,HttpDataFile> files = new HashMap<String,HttpDataFile>();
    private boolean isKeepAlive;
    
    private HttpResp resp = new HttpResp(this);
    
    private HttpReq(HttpServiceConf conf, ChannelHandlerContext ctx) {
      this.conf = conf;
      this.ctx  = ctx;
    }
    
    
    public  String remoteIp() {
      if (remoteIp != null) {
        return remoteIp.isEmpty() ? null : remoteIp;
      }

      String ip = headers.get("X-Forwarded-For");
      if (ip == null || ip.isEmpty() || ip.equals("unknown")) {
        ip = headers.get("Proxy-Client-IP");
      }
      if (ip == null || ip.isEmpty() || ip.equals("unknown")) {
        ip = headers.get("WL-Proxy-Client-IP");
      }
      if (ip == null || ip.isEmpty() || ip.equals("unknown")) {
        try {
          ip = ((InetSocketAddress)ctx.channel().remoteAddress())
              .getAddress().getHostAddress();
        } catch (Exception e) {}
      }
      
      if (ip == null || ip.isEmpty() || ip.equals("unknown")) {
        ip = "";
      } else {
        ip = ip.split(",")[0];
      }
      remoteIp = ip;
      return remoteIp.isEmpty() ? null : remoteIp;
    }
    
    public  String method() {
      return method;
    }
    
    public  String uri() {
      return uri;
    }
    
    public  Map<String,String> headers() {
      return headers;
    }
    
    public  Map<String,String> params() {
      return params;
    }
    
    public  Map<String,HttpDataFile> files() {
      return files;
    }
    
    public  HttpResp resp() {
      return resp;
    }
    
  }
  
  public  static class HttpResp {
    
    private HttpReq req;
    public  int status = 200;
    private Map<String,String> headers;
    private byte[] content;
    private String contentStr;
    private boolean flushed = false;
    
    private HttpResp(HttpReq req) {
      this.req = req;
    }
    
    
    public  HttpResp status(int status) {
      this.status = status;
      return this;
    }
    
    public  HttpResp header(String name, String value) {
      ensureHeaders();
      headers.put(name, value);
      return this;
    }
    
    public  HttpResp content(byte[] content) {
      this.content = content;
      return this;
    }
    
    public  HttpResp content(String content) {
      this.contentStr = content;
      return this;
    }
    
    public  void flush() {
      if (flushed) {return;}
      flushed = true;
      
      if (content == null) {
        content = contentStr != null ? contentStr.getBytes(Utf8) : new byte[]{};
      }
      FullHttpResponse resp = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(status),
          Unpooled.wrappedBuffer(content));
      
      //headers
      if (req.isKeepAlive) {
         resp.headers().set(
             HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      }
      
      if (req.conf.cors != null) {
        resp.headers().set("Access-Control-Allow-Origin", req.conf.cors);
      }
      
      resp.headers().set(
          HttpHeaderNames.CONTENT_LENGTH, resp.content().readableBytes());
      
      if (headers != null) {
        for (Entry<String,String> e : headers.entrySet()) {
          resp.headers().set(e.getKey(), e.getValue());
        }
      }
      
      
      req.ctx.write(resp);
      req.ctx.flush();
    }
    
    
    private void ensureHeaders() {
      if (headers == null) {
        headers = new TreeMap<String,String>();
      }
    }
    
  }
  
  public  static class HttpDataFile {
    private String filename;
    private String contentType;
    private byte[] content;
    
    public HttpDataFile(
        String filename, String contentType, byte[] content) {
      this.filename = filename;
      this.contentType = contentType;
      this.content = content != null ? content : new byte[] {};
    }
    
    public  String filename() {
      return filename;
    }
    
    public  String contentType() {
      return contentType;
    }
    
    public  byte[] content() {
      return content;
    }
    
  }
  
  private class HttpRequestHandler extends ChannelInboundHandlerAdapter {

    public  void channelRead(ChannelHandlerContext ctx, Object msg)
    throws Exception {
      if (msg instanceof FullHttpRequest) {
        FullHttpRequest msg1 = (FullHttpRequest) msg;
        HttpReq req = new HttpReq(conf, ctx);
        
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
                req.files.put(f.getName(), new HttpDataFile(
                    f.getFilename(), f.getContentType(), fcontent));
              } else {
                Attribute data2 = (Attribute) data;
                req.params.put(data2.getName(), data2.content().toString(Utf8));
              }
            }
            decoder.destroy();
          }
        }
        //msg1.content().release(); //realease content CompositeByteBuf

        if (conf.logReq) {
          logger.info("req: {}, {}", req.uri, req.params);
        }

        if (conf.reqHandler != null) {
          conf.reqHandler.req(req);
        }
      }
    }

    public  void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      logger.error("HttpRequestHandler.fail", cause);
      ctx.close();
    }

  }
  
  private  static void decodeParam(String raw, Map<String,String> map) {
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
      logger.info("decodeParam.fail", e);
    }
  }
  
}
