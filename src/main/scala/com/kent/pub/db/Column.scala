package com.kent.pub.db

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.kent.pub.db.Column.DataType.DataType
/**
  * @author kent
  * @date 2019-07-19
  * @desc
  *
  **/
class Column(val columnName: String, val columnType: DataType, val dataLength: Int, val precision: Int){

}

object Column {
  def apply(columnName: String, columnType: DataType, dataLength: Int, precision: Int): Column = new Column(columnName, columnType, dataLength, precision)

  object DataType extends Enumeration {
    type DataType = Value
    val STRING, NUMBER  = Value
  }



}
