package com.kent.test

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.IOException
import java.io.ByteArrayInputStream

class CloneTest extends Serializable{
  @transient var a = 1;
  var b = new Child();
  def myclone[A <: CloneTest]:A = {
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
      outer.asInstanceOf[A]
  }
  class Child extends Serializable{
    var a = "aaaa"
  }
}

class CloneTestC extends CloneTest{
  var c = "ccccc"
}

object CloneTest extends App{
  var c = new CloneTestC;
  c.b.a = "11111"
  c.c = "22222"
  var c2 = c.myclone[CloneTestC]
  println(c2.b.a)
  println(c2.c)
}