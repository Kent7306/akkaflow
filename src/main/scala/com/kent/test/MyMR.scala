package com.kent.test

object MyMR extends App{
  def run(records: List[Record], mapper: Record => KV, reducer: (String, List[Record]) => List[KV]): List[KV] = {
    val mResult = records.map { rec => 
      mapper(rec)
    }.toList
    //shuffle
    val mTmp = scala.collection.mutable.Map[String, List[Record]]()
    mResult.foreach { case KV(k,v) =>
      if(mTmp.get(k).isEmpty) {
        mTmp += (k -> List(v))
      }else{
        mTmp(k) = mTmp(k) :+ v
      }
    }
    val rResult = mTmp.flatMap{ case(k,values) =>
      reducer(k,values)
    }.toList
    rResult
  }
  
  val aTable = """
    1,孙老师
    2,杨主任
    3,谭校长
    """
  val bTable = """
    小明,1
    小刘,1
    小张,3
    """
  val a = aTable.split("\n").filter { _.trim() != "" }.map { _.trim().split(",").toList }.toList
  val b = bTable.split("\n").filter { _.trim() != "" }.map { _.trim().split(",").toList }.toList
  println(a)
  
  val data1 = a.map { x => Record(x.mkString(","),1) }
  val data2 = b.map { x => Record(x.mkString(","),2) }
  val data = data1 ++ data2
  
  def mapper(record: Record): KV = {
    record match {
      case x if x.tag == 1 => KV(record.data.split(",")(0),Record(x.data,1))
      case x if x.tag == 2 => KV(record.data.split(",")(1),Record(x.data,2))
    }
  }
  
  def reducer(key: String, values: List[Record]):List[KV] = {
    //笛卡尔积
    val(l1, l2) = values.partition { x => x.tag == 1 }
    val buffer = l1.flatMap{ x => l2.map { y => KV(key,Record(x.data+","+y.data, 1)) } }.toList
    buffer
  }
  
  println(run(data,mapper,reducer))
  
  
  case class Record(data: String, tag: Int)
  case class KV(key: String, value: Record)
}