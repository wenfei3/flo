package prj.flo.demo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import prj.flo.demo.dao.UserDao;
import prj.flo.demo.model.Result;
import prj.flo.demo.model.User;

public class UserServiceImpl implements UserService {
  
  private static final Logger logger =
      LoggerFactory.getLogger(UserServiceImpl.class);
  private UserDao userDao = UserDao.instance();


  @Override
  public Result addUser(User user, String auth) {
    //add a user
    //only admin(id:-1) can add user
    
    try {
      Result br = Result.BAD_REQUEST;
      if (user == null) {return br.msg("invalid user");}
      if (user.nickname == null || user.nickname.isEmpty()) {
        return br.msg("invalid user.nickname");
      }
      if (user.gender == null) {
        return br.msg("invalid user.gender");
      }
      
      Long reqUserId = userIdByAuth(auth);
      if (reqUserId == null || reqUserId != -1) {
        return Result.UNAUTHORIZED;
      }
      
      user.id = null;
      user.createTime = System.currentTimeMillis();
      user.modifyTime = user.createTime;
      int count = userDao.add(user);
      
      if (count == 0) {
        logger.info("addUser.fail.insert_result_zero");
        return Result.FAIL;
      }
      
      return Result.succ(user);
      
    } catch (Exception e) {
      logger.error("addUser.fail", e);
      return Result.FAIL;
    }
  }

  public Result modUser(User user, String auth) {
    //modify user by id
    //only self can modify nickname
    //createTime can not be modified
    
    try {
      Result br = Result.BAD_REQUEST;
      if (user == null) {return br.msg("invalid user");}
      if (user.id == null) {
        return br.msg("invalid user.id");
      }
      if (user.nickname == null || user.nickname.isEmpty()) {
        return br.msg("invalid user.nickname");
      }
      
      Long reqUserId = userIdByAuth(auth);
      if (reqUserId == null || reqUserId != user.id) {
        return Result.UNAUTHORIZED;
      }
      
      user.createTime = null;
      user.modifyTime = System.currentTimeMillis();
      int count = userDao.mod(user);
      
      if (count == 0) {
        return Result.USER_NOT_EXIST;
      }
      
      return Result.SUCC;
      
    } catch (Exception e) {
      logger.error("modUser.fail", e);
      return Result.FAIL;
    }
  }

  public Result getUser(Long id, String auth) {
    //get user by id
    //everyone can get
    
    try {
      if (id == null) {
        return Result.BAD_REQUEST.msg("invalid id");
      }
      
      User user = userDao.getById(id);
      
      return Result.succ(user);
      
    } catch (Exception e) {
      logger.error("getUser.fail", e);
      return Result.FAIL;
    }
  }
  
  private Long userIdByAuth(String auth) {
    try {
      return Long.valueOf(auth);
    } catch (Exception e) {
      return null;
    }
  }

}
