package com.kent.pub

/**
  * @author kent
  * @date 2019-07-04
  * @desc 通用执行结果类
  *
  **/
class Result(val isSuccess: Boolean,msg: String, dataOpt: Option[Any]) extends Serializable{
  /**
    * 执行信息
    * @return
    */
  def message: String = msg

  /**
    * 得到返回数据Option
    * @return
    */
  def dataOption = dataOpt

  /**
    * 是否失败
    * @return
    */
  def isFail: Boolean = !isSuccess

  /**
    * 是否有执行信息
    * @return
    */
  def isHasMessage: Boolean = if (msg == null) false else true

  /**
    * 是否有返回数据
    * @return
    */
  def isHasData: Boolean = dataOpt.isDefined

  /**
    * 是否执行成功并且返回有数据
    * @return
    */
  def isSuccessAndHasData: Boolean = if(isSuccess && dataOpt.isDefined) true else false

  /**
    * 把返回数据Option转换为指定类型Option
    * @tparam A
    * @return
    */
  def toDataOpt[A]: Option[A] = if (dataOpt.isDefined) Some(dataOpt.get.asInstanceOf[A]) else None

  /**
    * 把返回数据Option转换为指定类型
    * @tparam A
    * @return
    */
  def data[A]: A = if (dataOpt.isDefined) dataOpt.get.asInstanceOf[A] else throw new Exception("data值不存在")

  /**
    * 转换为json字符串
    * @return
    */
  def toJsonString: String = {
    val resultStr = if (isSuccess) "success" else "fail"
    val bean = ResultBean(resultStr, msg, dataOption.orNull)
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write

    implicit val formats = Serialization.formats(NoTypeHints)
    write(bean)
  }

  override def toString: String = toJsonString
}

case class ResultBean(result:String, msg: String, data: Any)
/**
  * 成功执行结果类
  * @param msg
  * @param dataOpt
  */
class SucceedResult(msg: String, dataOpt: Option[Any]) extends Result(true, msg, dataOpt)
object SucceedResult{
  def apply(msg: String, dataOpt: Option[Any]): SucceedResult = new SucceedResult(msg, dataOpt)

  def apply(dataOpt: Option[Any]): SucceedResult = new SucceedResult(null, dataOpt)

  def apply(msg: String): SucceedResult = new SucceedResult(msg, None)

  def apply(): SucceedResult = new SucceedResult(null, None)

  def unapply(r: SucceedResult): Option[(String, Option[Any])] = {
    if (r == null){
      None
    } else {
      Some(r.message, r.dataOption)
    }
  }
}

/**
  * 失败执行结果类
  * @param msg
  * @param dataOpt
  */
class FailedResult(msg: String, dataOpt: Option[Any]) extends Result(false, msg, dataOpt)
object FailedResult{
  def apply(msg: String, dataOpt: Option[Any]): FailedResult = new FailedResult(msg, dataOpt)

  def apply(dataOpt: Option[Any]): FailedResult = new FailedResult(null, dataOpt)

  def apply(): FailedResult = new FailedResult(null, None)

  def apply(msg: String): FailedResult = new FailedResult(msg, None)

  def unapply(r: FailedResult): Option[(String, Option[Any])] = {
    if (r == null){
      None
    } else {
      Some(r.message, r.dataOption)
    }
  }
}
