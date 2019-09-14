package com.kent.workflow.node.action.transfer
/**
  * @author kent
  * @date 2019-07-17
  * @desc
  *
  **/
class DataShare(batchNum: Int) extends Serializable {
  private val batches = scala.collection.mutable.ArrayBuffer[GenericBatch]()
  //共插入多少条数据
  private var persistCnt = 0

  var exceptionOpt: Option[Throwable] = None

  def put(data: GenericBatch): Unit ={
    batches.synchronized{  //..
      while (batches.size == batchNum){
        batches.wait()
      }
      batches += data
      batches.notifyAll()
    }
  }

  def clear(): Unit = {
    batches.synchronized{
      batches.clear()
      batches.notifyAll()
    }
  }

  def get(): GenericBatch = {
    batches.synchronized{
      while (batches.isEmpty){
        batches.wait()
      }
      val a = batches.head
      a match {
        case TransferEnd() =>
        case TransferPoison() =>
        case b:DataBatch => batches -= b
      }
      batches.notifyAll()
      a
    }
  }

  def incrPersistCnt(incr: Int) = {
    persistCnt.synchronized{
      persistCnt = persistCnt + incr
    }
  }
  def getPersistCnt(): Int = {
    persistCnt.synchronized{
      this.persistCnt
    }
  }

  def handleException(e: Exception): Unit = {
    exceptionOpt.synchronized{
      exceptionOpt = Some(e)
      this.clear()
      this.put(TransferPoison())
    }
  }
}

class GenericBatch()
case class DataBatch(records: List[List[String]]) extends GenericBatch
case class TransferEnd() extends GenericBatch
case class TransferPoison() extends GenericBatch