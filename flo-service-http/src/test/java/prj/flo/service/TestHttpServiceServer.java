package prj.flo.service;

public class TestHttpServiceServer {
  
  public  static void main(String[] args) throws Exception {
    
    //ATTENTION
    //use javac -parameters in java1.8 to keep parameter name in class file
    
    HttpServiceServer server = HttpServiceServer.server();
    server.start(new UserServiceImpl());
    
    //open http://127.0.0.1:8200/i/u/getUser?id=11
  }

}
