package prj.flo.service;

/**
 * Service Server interface.
 *  
 * @author Leon Dong
 */
interface ServiceServer {
  
  <I> void start(I service);
  
  <I> void start(I service, Class<I> itface);
  
  <I> void stop (I service);
  
  <I> void stop (I service, Class<I> itface);
  
}
