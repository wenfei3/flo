package prj.flo.demo.dao;

import prj.flo.db.Db;
import prj.flo.demo.model.User;

public class UserDao {
  
  private static final UserDao instance = new UserDao();
  
  public  static UserDao instance() {
    return instance;
  }


  private Db db = Db.getInstance("prj.flo.demo.flo_demo");
  
  private UserDao() {
    initTable();
  }
  
  private void initTable() {
    String sql = null;
    
    sql = "create table if not exists `user` ("
        + "  `id`           bigint       not null auto_increment,"
        + "  `create_time`  bigint       not null,"
        + "  `modify_time`  bigint       not null,"
        + "  `nickname`     varchar(100) not null,"
        + "  `gender`       tinyint      not null,"
        + "  primary key (`id`),"
        + "  index i_user_nickname (`nickname`)"
        + ") engine=InnoDB default charset=utf8mb4";
    db.update(new Db.Update().sql(sql));
  }
  
  public  int  add(User user) {
    return db.add(user);
  }
  
  public  int  mod(User user) {
    return db.mod(user);
  }
  
  public  User getById(long id) {
    return db.get(new User(id));
  }
  
}
