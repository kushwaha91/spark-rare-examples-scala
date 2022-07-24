package com.sparkrareexamples

import org.apache.spark.sql.types.{BinaryType, DataType, UserDefinedType}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

class ElementRecord extends UserDefinedType[ElementRoot] {

 override def sqlType: DataType = BinaryType

 override def serialize(obj: ElementRoot): Any = {
  val bos = new ByteArrayOutputStream
  val oos = new ObjectOutputStream(bos)
  oos.writeObject(obj)
  bos.toByteArray
 }

 override def deserialize(datum: Any): ElementRoot = {
  val bis = new ByteArrayInputStream(datum.asInstanceOf[Array[Byte]])
  val ois = new ObjectInputStream(bis)
  val obj = ois.readObject
  obj.asInstanceOf[ElementRoot]
 }

 override def userClass : Class[ElementRoot] = classOf[ElementRoot]
 }
