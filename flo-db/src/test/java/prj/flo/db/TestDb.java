package prj.flo.db;

import java.util.List;

public class TestDb {
  
  public  static class User {
    public  long   userId;
    public  long   ctime;
    public  String nickname;
    public  int    gender;
    public  int    birthday;
    public  String birthdayStr;
    public  String json;
    
    public  int    getBirthday() {
      return birthday;
    }
    
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
      insert into user(ctime,nickname,gender,birthday,json) values
        (101, 'name1', 1, 20000101, '{}'),
        (103, 'name2', 2, 20000102, '{"x":123}'),
        (112, 'name3', 1, 20000103, '{}');
     */
    
    Db db = Db.getInstance("test1");
    Db.Query<User> q = new Db.Query<User>();
    q.sql = "select * from user limit 2";
    q.ormClass = User.class;
    List<User> users = db.query(q);
    for (User u : users) {
      System.out.println(u);
    }
  }

}
