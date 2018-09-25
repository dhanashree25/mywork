package com.diceplatform.brain

import org.apache.hadoop.mapred._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

class SingleJSONLineInputFormat extends FileInputFormat[LongWritable, Text] {
  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter): RecordReader[LongWritable, Text] = {
    reporter.setStatus(split.toString)
    new SingleJSONLineRecordReader(job, split.asInstanceOf[FileSplit])
  }
}