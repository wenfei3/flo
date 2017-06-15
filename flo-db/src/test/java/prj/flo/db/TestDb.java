package prj.flo.db;

import java.util.ArrayList;
import java.util.List;

public class TestDb {
  
  @Db.OrmClass(table = "user")
  public  static class User {
    @Db.OrmField(id = true, autoIncre = true)
    public  Long    id;
    public  Long    ctime;
    public  String  nickname;
    public  Integer gender;
    public  Integer birthday;
    @Db.OrmIgnore
    public  String  birthdayStr;
    public  String  json;
    
    public  void   setBirthday(int birthday) {
      this.birthday = birthday;
      this.birthdayStr = "" + (birthday / 10000)
          + "." + ((birthday % 10000) / 100)
          + "." + (birthday % 100);
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
  
  public  static void main(String[] args) throws Exception {
    /*
      create database test_flo_db default charset=utf8;
      use test_flo_db;
      create table `user` (
        `id`           bigint       not null auto_increment,
        `ctime`        bigint       not null,
        `nickname`     varchar( 20)         ,
        `gender`       tinyint      not null,
        `birthday`     int          not null,
        `json`         varchar(4095)        ,
        primary key (`id`),
        index i_user_nickname (`nickname`)
      ) engine=InnoDB default charset=utf8;
     */
    
    
    Db db = Db.getInstance("test_flo_db");
    
    
    //add some users if no data
    Long count = db.querySingle(new Db.Query<Long>()
        .sql("select count(*) from user"));
    if (count < 5) {
      System.out.println("add users.");
      
      for (int i = 0; i < 2; i++) {
        User user = new User();
        user.ctime    = System.currentTimeMillis();
        user.nickname = "name" + i;
        user.gender   = 1;
        user.birthday = 20010203;
        user.json     = "{}";
        db.add(user);
      }
      
      Db.Batch batch = new Db.Batch()
          .sql("insert into user(ctime,nickname,gender,birthday,json)"
              + " values(?,?,?,?,?)");
      List<Object[]> params = new ArrayList<Object[]>();
      for (int i = 2; i < 5; i++) {
        User user = new User();
        user.ctime    = System.currentTimeMillis();
        user.nickname = "name" + i;
        user.gender   = 1;
        user.birthday = 20010203;
        user.json     = "{}";
        params.add(new Object[] {
            user.ctime, user.nickname, user.gender, user.birthday, user.json
        });
      }
      batch.params(params);
      db.batch(batch);
    }
    
    
    //list users
    List<User> users = db.list(User.class, new Db.OpParam().limit(1, 3));
    System.out.println("list users:");
    for (User u : users) {
      System.out.println(u);
    }

    
    //get, mod, del user
    Long userIdMin = db.querySingle(new Db.Query<Long>()
        .sql("select min(id) from user"));
    
    User userId = new User();
    userId.id = userIdMin;
    User user = db.get(userId);
    System.out.println("get user:");
    System.out.println(user);
    
    User user2 = new User();
    user2.id = user.id;
    user2.gender = 1 - user.gender;
    db.mod(user2);
    System.out.println("mod user:");
    System.out.println(db.get(userId));
    
    db.del(user2);
    System.out.println("del user:");
    System.out.println(db.get(userId));
  }

}
