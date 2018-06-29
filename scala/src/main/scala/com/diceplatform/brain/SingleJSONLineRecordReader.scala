package com.diceplatform.brain

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred._
import org.apache.hadoop.io._

class SingleJSONLineRecordReader(job: Configuration, split: FileSplit) extends RecordReader[LongWritable, Text] {
<<<<<<< HEAD
=======
  val BUFFER_SIZE = "com.diceplatform.brain.input.singlejsonlinerecordreader.buffer.size"

>>>>>>> release_candidate
  private val fileStart = split.getStart
  private val fileFinish = fileStart + split.getLength
  private var filePosition = fileStart

  private val file = split.getPath
  private val fileIn = file.getFileSystem(job).open(file)

<<<<<<< HEAD
  private val buffer = new Array[Byte](10)
=======
  private val buffer = new Array[Byte](job.getInt(BUFFER_SIZE, 1024 * 1024)) // 1 MB
>>>>>>> release_candidate
  private var bufferLength = 0
  private var bufferPosition = 0

  private var startPosition = 0

  private val OPEN_PARENTHESIS = '{'
  private val CLOSE_PARENTHESIS = '}'

  private var currentCharacter: Byte = '\0'
  private var lastCharacter: Byte = '\0'

  private val CR: Byte = '\r'
  private val LF: Byte = '\n'

  fileIn.seek(fileStart)

  private def readJSON(value: Text): Int = {
    // Rest previous value without clearing the underlying byte array
    value.clear()

    var bytesConsumed = 0

    var break = false
    var found = false
    var newLineLength = 0
    // Read a portion of the file in bit by bit
    do {
      startPosition = bufferPosition

      // If we have read all of the previous buffer, read in a new buffer.
      if (bufferPosition >= bufferLength) {
        bufferPosition = 0
        startPosition = 0

        bufferLength = fileIn.read(buffer)

        if (bufferLength <= 0) {
          break = true
        }
      }

      // Search buffered byte array for delimiters
      while (bufferPosition < bufferLength && !found) {
        currentCharacter = buffer(bufferPosition)

        if (currentCharacter == LF) {
          newLineLength = 1
          found = true
        }

        // Object found in the middle of the line
        if (lastCharacter    == CLOSE_PARENTHESIS &&
            currentCharacter == OPEN_PARENTHESIS) {
          found = true
        }

        lastCharacter = currentCharacter
        bufferPosition += 1
      }

      // Read length may be less than the buffer position as the buffer contains parts to two objects
      var readLength = bufferPosition - startPosition

      // Reduce read length and buffer position otherwise it includes opening parenthesis
      if (found && lastCharacter != LF) {
        readLength -= 1
        bufferPosition -= 1
      }

      bytesConsumed += readLength

      val appendLength = readLength - newLineLength
      if (appendLength > 0) {
        value.append(buffer, startPosition, appendLength)
      }
    } while (!break && !found)

    bytesConsumed
  }

  override def next(key: LongWritable, value: Text): Boolean = {
    value.clear()

    // Read file
    while (filePosition <= fileFinish) {
      // Set record as file position, this will change when we find a record
      key.set(filePosition)

      // Read JSON object
      val newSize = readJSON(value)

      filePosition += newSize

      // If newSize is 0 indicates incomplete
      return newSize != 0
    }

    false
  }

  override def createKey(): LongWritable = new LongWritable

  override def createValue(): Text = new Text

  override def getPos: Long = filePosition

  override def close(): Unit = {
    fileIn.close()
  }

  override def getProgress: Float = {
    if (fileStart == fileFinish) {
      0.0f
    } else {
      Math.min(1.0f, (filePosition - fileStart) / (fileFinish - fileStart).toFloat)
    }
  }
}
