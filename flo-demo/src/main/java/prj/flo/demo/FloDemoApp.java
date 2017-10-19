package prj.flo.demo;

import prj.flo.demo.service.UserServiceImpl;
import prj.flo.service.HttpServiceServer;

public class FloDemoApp {

  public  static void main(String[] args) {
    HttpServiceServer hss = HttpServiceServer.server();
    hss.start(new UserServiceImpl());
    
    //add user
    //http://127.0.0.1:8200/i/u/addUser?user=%7B%22nickname%22%3A%22name1%22%2C%22gender%22%3A1%7D&auth=-1
    //get user
    //http://127.0.0.1:8200/i/u/getUser?id=1
    //mod user
    //http://127.0.0.1:8200/i/u/modUser?user=%7B%22id%22%3A1%2C%22nickname%22%3A%22name1b%22%7D&auth=1
    //get user
    //http://127.0.0.1:8200/i/u/getUser?id=1
  }

}
