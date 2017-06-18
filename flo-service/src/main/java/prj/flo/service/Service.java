package prj.flo.service;

import java.util.TreeMap;

/**
 * Service.
 *  
 * @author Leon Dong
 */
interface Service {
  
  public  static class ServiceException extends RuntimeException {
    
    private static final long serialVersionUID = -282210561220562769L;

    public ServiceException() {
      super();
    }
    
    public ServiceException(String msg) {
      super(msg);
    }
    
    public ServiceException(Throwable cause) {
      super(cause);
    }
    
    public ServiceException(String msg, Throwable cause) {
      super(msg, cause);
    }
    
  }
  
  public  static abstract class Req {
    
    TreeMap<String,Object> attrs  = new TreeMap<String,Object>();
    TreeMap<String,Object> params = new TreeMap<String,Object>();
    
    public  Object attr(String name) {
      return attrs.get(name);
    }
    
    public  void   attr(String key, Object value) {
      attrs.put(key, value);
    }
    
    public  Object param(String name) {
      return params.get(name);
    }
    
    public  abstract String remoteIp();
    
  }
  
}
