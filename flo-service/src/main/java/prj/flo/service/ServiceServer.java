package prj.flo.service;

/**
 * Service Server interface.
 *  
 * @author Leon Dong
 */
interface ServiceServer {
  
  <I> void start(I service, Class<I> itface);
  
  <I> void start(I service);
  
  void stop (String name);
  
}
