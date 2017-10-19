package prj.flo.demo.model;

public class Result {

  private int    code;
  private String msg;
  private Object data;
  
  private Result() {}
  
  private Result(int code) {
    this.code = code;
  }
  
  private Result(int code, String msg) {
    this.code = code;
    this.msg  = msg;
  }
  
  
  public  int    getCode() {
    return code;
  }
  
  public  String getMsg() {
    return msg;
  }
  
  public  Object getData() {
    return data;
  }
  
  public  Result msg(String msg) {
    Result r = new Result();
    r.code = code;
    r.msg  = msg;
    r.data = data;
    return r;
  }
  
  public  Result data(Object data) {
    Result r = new Result();
    r.code = code;
    r.msg  = msg;
    r.data = data;
    return r;
  }
  
  
  public  static Result succ(Object data) {
    return SUCC.data(data);
  }
  
  public  static Result fail(String msg) {
    return FAIL.msg(msg);
  }




  // results
  
  public  static final Result SUCC = new Result(0);
  public  static final Result FAIL = new Result(1, "fail");
  
  public  static final Result BAD_REQUEST  = new Result(400, "bad request");
  public  static final Result UNAUTHORIZED = new Result(401, "unauthorized");

  public  static final Result USER_NOT_EXIST = new Result(1001,
      "user not exists");

}
