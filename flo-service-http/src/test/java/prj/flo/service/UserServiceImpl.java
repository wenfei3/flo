package prj.flo.service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class UserServiceImpl implements UserService {

  public Future<User> getUser(User reqUser, Long id) {
    User fake = new User();
    fake.id = id;
    fake.nickname = "fake";
    fake.gender   = 1;
    fake.setBirthday(20110203);
    
    if (reqUser == null || reqUser.id != id) {
      fake.gender = null;
      fake.setBirthday(null);
    }
    
    CompletableFuture<User> r = new CompletableFuture<UserService.User>();
    r.complete(fake);
    return r;
  }

  public User getUser2(User reqUser, Long id) {
    try {
      return getUser(reqUser, id).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void modUser(User user) {
    //...
  }

}
