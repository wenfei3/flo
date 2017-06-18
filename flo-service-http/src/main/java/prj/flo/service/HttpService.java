package prj.flo.service;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public interface HttpService {

  static class HttpConf {

//    HttpReqHandler reqHandler;
    boolean        logReq = true;
    int            port = 8080;
    int            contentLengthMax = 10 * 1024 * 1024;
    String         cors = null;
    String         uriPrefix = "/";
//    public  HttpConf() {}
//    
//    public  HttpConf(HttpConf conf1) {
//      this.reqHandler = conf1.reqHandler;
//      this.logReq     = conf1.logReq;
//      this.port       = conf1.port;
//      this.contentLengthMax = conf1.contentLengthMax;
//      this.cors       = conf1.cors;
//    }
    
    public  HttpConf logReg(boolean logReq) {
      this.logReq = logReq;
      return this;
    }
    
    public  HttpConf port(int port) {
      this.port = port;
      return this;
    }
    
//    public  HttpConf reqHandler(HttpReqHandler reqHandler) {
//      this.reqHandler = reqHandler;
//      return this;
//    }
    
    public  HttpConf contentLengthMax(int contentLengthMax) {
      this.contentLengthMax = contentLengthMax;
      return this;
    }
    
    public  HttpConf cors(String site) {
      this.cors = site;
      return this;
    }
    
  }

  public  static class HttpReq extends Service.Req {
    
    ChannelHandlerContext ctx;
    HttpConf conf;
    HttpResp resp = new HttpResp(this);
    
    String remoteIp;
    String method;
    String uri;
    Map<String,String> headers = new HashMap<String,String>();
    Map<String,String> params = new HashMap<String,String>();
    Map<String,HttpDataFile> files = new HashMap<String,HttpDataFile>();
    boolean isKeepAlive;
    
    HttpReq(ChannelHandlerContext ctx) {
      this.ctx  = ctx;
    }
    

    public  HttpResp resp() {
      return resp;
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
    
  }
  
  public  static class HttpResp {
    
    private static final Charset Utf8 = Charset.forName("UTF-8");
    
    HttpReq            req;
    int                status = 200;
    Map<String,String> headers;
    byte[]             content;
    String             contentStr;
    boolean            flushed = false;
    
    private HttpResp(HttpReq req) {
      this.req = req;
    }
    

    public  int status() {
      return status;
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
      
      if (req.conf != null && req.conf.cors != null) {
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
  
}
