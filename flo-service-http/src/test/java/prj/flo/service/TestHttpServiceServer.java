package prj.flo.service;

import prj.flo.service.HttpServiceServer.HttpReq;
import prj.flo.service.HttpServiceServer.HttpReqHandler;
import prj.flo.service.HttpServiceServer.HttpServiceConf;

public class TestHttpServiceServer {
  
  public  static void main(String[] args) throws Exception {
    final HttpServiceServer server = new HttpServiceServer(new HttpServiceConf()
        .port(8080)
        .reqHandler(new HttpReqHandler() {
          public void req(HttpReq req) {
            System.out.println("");
            System.out.println("req.method:" + req.method());
            System.out.println("req.uri   :" + req.uri());
            System.out.println("req.params:" + req.params());

            try {
              Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            String res = "<html>"
                + "<body>"
                + "<form action='/t1?f0=z' method='post'>"
                + "<input type='text' name='f1是' value='aaa'/>"
                + "<input type='text' name='f2' value='b'/>"
                + "<input type='text' name='f2' value='c'/>"
                + "<input type='submit'/>"
                + "</form>"
                + "</body>"
                + "</html>";
            req.resp().header("Content-Type", "text/html")
              .content(res)
              .flush();
          }
        }));
    server.start();
    System.out.println("server started");
  }

}
