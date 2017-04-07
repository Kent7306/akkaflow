package com.kent.pub

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.io.IOException

/**
 * 深度克隆特质
 */
trait DeepCloneable[A] extends Serializable{
  /**
   * 克隆接口,并指定下界
   */
  def deepClone[B <: A]():B = {
      var outer: Any = null
      try { // 将该对象序列化成流,因为写在流里的是对象的一个拷贝，而原对象仍然存在于JVM里面。所以利用这个特性可以实现对象的深拷贝
          val baos = new ByteArrayOutputStream()
          val oos = new ObjectOutputStream(baos)
          oos.writeObject(this)
          // 将流序列化成对象
          val bais = new ByteArrayInputStream(baos.toByteArray())
          val ois = new ObjectInputStream(bais)
          outer = ois.readObject();
      } catch{
        case e:IOException => e.printStackTrace()
        case e:Exception => e.printStackTrace()
      }
      outer.asInstanceOf[B]
  }
  /**
   * 克隆接口，返回自身类型
   */
  def deepClone():A = {
      deepClone[A]
  }
}