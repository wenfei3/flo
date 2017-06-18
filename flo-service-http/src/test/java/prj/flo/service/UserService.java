package prj.flo.service;

import java.util.concurrent.Future;

public interface UserService {

  public  static class User {
    public  Long    id;
    public  Long    ctime;
    public  String  nickname;
    public  Integer gender;
    public  Integer birthday;
    public  String  birthdayStr;
    public  String  json;
    
    public  void   setBirthday(Integer birthday) {
      this.birthday = birthday;
      
      if (birthday == null) {
        this.birthdayStr = null;
      } else {
        this.birthdayStr = "" + (birthday / 10000)
            + "." + ((birthday % 10000) / 100)
            + "." + (birthday % 100);
      }
    }
    
    public  String toString() {
      return "{id:" + id
          + ", ctime:" + ctime
          + ", nickname:" + nickname
          + ", gender:" + gender
          + ", birthday:" + birthday
          + ", birthdayStr:" + birthdayStr
          + ", json:" + json
          + "}";
    }
  }
  
  Future<User> getUser(User userReq, Long id);
  
  User getUser2(User userReq, Long id);
  
  void modUser(User user);

}
