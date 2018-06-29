package com.diceplatform.brain

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred._
import org.apache.hadoop.fs._
import java.io.File

import org.scalatest._

class SingleJSONLineRecordReaderSpec extends FlatSpec {
  "an empty file" should "return no records" in {
    val conf = new Configuration()
    val job = new JobConf(conf)

    val testFileUrl = getClass.getResource("/empty.json")
    val testFile = new File(testFileUrl.getFile)
    val testFilePath = new Path(testFile.getAbsolutePath)
    val testFileSize = testFile.length()

    val hosts: Array[String] = null
    val split = new FileSplit(testFilePath, 0, testFileSize, hosts)

    val reader = new SingleJSONLineRecordReader(job, split)

    val key = reader.createKey()
    val value = reader.createValue()

    assert(!reader.next(key, value))
    assert(reader.getPos == 0)
    assert(reader.getProgress == 0)
    assert(key.get == 0)
    assert(value.getLength == 0)
    assert(value.toString == "")
    reader.close()
  }

  "a file with a single object" should "return a single record" in {
    val conf = new Configuration()
    val job = new JobConf(conf)

    val testFileUrl = getClass.getResource("/single.json")
    val testFile = new File(testFileUrl.getFile)
    val testFilePath = new Path(testFile.getAbsolutePath)
    val testFileSize = testFile.length()

    val hosts: Array[String] = null
    val split = new FileSplit(testFilePath, 0, testFileSize, hosts)

    val reader = new SingleJSONLineRecordReader(job, split)

    val key = reader.createKey()
    val value = reader.createValue()

    assert(reader.next(key, value))
    assert(reader.getPos == testFileSize)
    assert(reader.getProgress == 1.0)
    assert(key.get == 0)
    assert(value.getLength == testFileSize)
    assert(value.toString == """{"foo": "bar"}""")
  }

  "a file with two single objects" should "return two records" in {
    val conf = new Configuration()
    val job = new JobConf(conf)

    val testFileUrl = getClass.getResource("/double.json")
    val testFile = new File(testFileUrl.getFile)
    val testFilePath = new Path(testFile.getAbsolutePath)
    val testFileSize = testFile.length()

    val hosts: Array[String] = null
    val split = new FileSplit(testFilePath, 0, testFileSize, hosts)

    val reader = new SingleJSONLineRecordReader(job, split)

    val key = reader.createKey()
    val value = reader.createValue()

    assert(reader.next(key, value))
    assert(value.toString == """{"foo": "bar"}""")
    assert(reader.getPos == 14)
    assert(reader.getProgress === (14.0 / testFileSize))
    assert(key.get == 0)
    assert(value.getLength == 14)

    assert(reader.next(key, value))
    assert(value.toString == """{"bar": "foo"}""")
    assert(reader.getPos == 28)
    assert(reader.getProgress === 1.0)
    assert(key.get == 14)
    assert(value.getLength == 14)

    assert(!reader.next(key, value))
    assert(value.toString == "")
    assert(reader.getProgress === 1.0)
    reader.close()
  }

  "a file with three nested objects" should "return three records" in {
    val conf = new Configuration()
    val job = new JobConf(conf)

    val testFileUrl = getClass.getResource("/complex-three.json")
    val testFile = new File(testFileUrl.getFile)
    val testFilePath = new Path(testFile.getAbsolutePath)
    val testFileSize = testFile.length()

    val hosts: Array[String] = null
    val split = new FileSplit(testFilePath, 0, testFileSize, hosts)

    val reader = new SingleJSONLineRecordReader(job, split)

    val key = reader.createKey()
    val value = reader.createValue()

    assert(reader.next(key, value))
    assert(value.toString == """{"payload":{"action":2,"data":{"cid":"19ade409-7278-4566-9b1f-989ab724f76a","startedAt":1528672255490,"device":"BROWSER","TA":"OK"},"video":11345,"progress":5654,"cid":"19ade409-7278-4566-9b1f-989ab724f76a"},"country":"US","town":"Sidney","clientIp":"75.186.76.212","realm":"dce.pbr","customerId":"qzbU","ts":"2018-06-11 00:25:54"}""")
    assert(reader.getPos == 331)
    assert(reader.getProgress === (331.0 / testFileSize).toFloat)
    assert(key.get == 0)
    assert(value.getLength == 331)

    assert(reader.next(key, value))
    assert(value.toString == """{"payload":{"action":2,"data":{"cid":"b55088a1-4530-4daf-a8e1-10e68486252e","startedAt":1528676388770,"device":"BROWSER","TA":"OK"},"video":11291,"progress":359,"cid":"b55088a1-4530-4daf-a8e1-10e68486252e"},"country":"FR","town":"ZZC","clientIp":"212.83.157.140","realm":"dce.fivb","customerId":"K.pM","ts":"2018-06-11 00:25:56"}""")
    assert(reader.getPos == (331 + 329))
    assert(reader.getProgress === ((331.0 + 329.0) / testFileSize).toFloat)
    assert(key.get == 331)
    assert(value.getLength == 329)

    assert(reader.next(key, value))
    assert(value.toString == """{"payload":{"action":2,"data":{"cid":"704c14a3-569b-4975-8ab6-3ebef320d6b0","startedAt":1528666440591,"device":"BROWSER","TA":"OK"},"video":11345,"progress":10315,"cid":"704c14a3-569b-4975-8ab6-3ebef320d6b0"},"country":"US","town":"Altus","clientIp":"160.3.57.114","realm":"dce.pbr","customerId":"f7bm","ts":"2018-06-11 00:25:58"}""")
    assert(reader.getPos == (331 + 329 + 330 + 1/* LF */))
    assert(reader.getProgress === ((331.0 + 329.0 + 330.0 + 1.0 /* LF */) / testFileSize).toFloat)
    assert(key.get == (331 + 329))
    assert(value.getLength == 330)

    assert(!reader.next(key, value))
    assert(value.toString == "")
    assert(reader.getProgress === 1.0)
  }
}
