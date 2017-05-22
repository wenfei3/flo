package prj.flo.db;

import java.util.List;

import prj.flo.db.Db.Update;

public class TestDb {
  
  @Db.OrmClass(table = "user")
  public  static class User {
    public  long   userId;
    public  long   ctime;
    public  String nickname;
    public  int    gender;
    public  int    birthday;
    @Db.OrmIgnore
    public  String birthdayStr;
    public  String json;
    
    public  void   setBirthday(int birthday) {
      this.birthday = birthday;
      this.birthdayStr = "" + (birthday / 10000)
          + "." + ((birthday % 10000) / 100)
          + "." + (birthday % 100);
    }
    
    public  String toString() {
      return "{userId:" + userId
          + ", ctime:" + ctime
          + ", nickname:" + nickname
          + ", gender:" + gender
          + ", birthday:" + birthday
          + ", birthdayStr:" + birthdayStr
          + ", json:" + json
          + "}";
    }
    
  }
  
  public  static void main(String[] args) throws Exception {
    /*
      create database test1 default charset=utf8;
      use test1;
      create table `user` (
        `user_id`      bigint       not null auto_increment,
        `ctime`        bigint       not null,
        `nickname`     varchar( 20)         ,
        `gender`       tinyint      not null,
        `birthday`     int          not null,
        `json`         varchar(4095)        ,
        primary key (`user_id`),
        index i_user_nickname (`nickname`)
      ) engine=InnoDB default charset=utf8;
     */
    
    Db db = Db.getInstance("test1");
    
    //add some users if no data
    Long count = db.querySingle(new Db.Query<Long>()
        .sql("select count(*) from user"));
    if (count < 5) {
      for (int i = 0; i < 5; i++) {
        User user = new User();
        user.ctime    = System.currentTimeMillis();
        user.nickname = "name" + i;
        user.gender   = 1;
        user.birthday = 20000104;
        user.json     = "{}";
        db.update(Update.add(user));
      }
    }
    
    //query users
    List<User> users = db.query(new Db.Query<User>(User.class)
        .sql("select * from user limit 5"));
    for (User u : users) {
      System.out.println(u);
    }
  }

}
