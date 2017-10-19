package prj.flo.demo.service;

import prj.flo.demo.model.Result;
import prj.flo.demo.model.User;

public  interface UserService {
  
  Result addUser(User user, String auth);
  
  Result modUser(User user, String auth);
  
  Result getUser(Long id, String auth);

}
