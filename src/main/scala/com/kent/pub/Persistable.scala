package com.kent.pub

import java.sql.Connection

trait Persistable[A] extends Daoable{
  /**
   * 保存或更新对象
   */
  def save(implicit conn: Connection): Boolean
  /**
   * 删除对象及相关联系对象
   */
  def delete(implicit conn: Connection): Boolean
  /**
   * 获取对象
   */
  def getEntity(implicit conn: Connection): Option[A]
}