package prj.flo.demo.model;

import prj.flo.db.Db;

@Db.OrmClass(table = "user")
public class User {

  @Db.OrmField(id = true, autoIncre = true)
  public  Long    id;
  public  Long    createTime;
  public  Long    modifyTime;
  public  String  nickname;
  public  Byte    gender;
  
  public  User() {}
  
  public  User(Long id) {
    this.id = id;
  }

}
