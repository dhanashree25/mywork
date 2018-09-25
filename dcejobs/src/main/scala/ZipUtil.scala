import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Path
import java.util.zip.ZipInputStream

import scala.collection.mutable.ListBuffer

object ZipUtil {
  val DEFAULT_BUFFER_SIZE: Int = 4096

  /**
    * Unzip a file from a source path to the destination folder path
    */
  def unzip(source: Path, destination: Path, bufferSize: Integer = DEFAULT_BUFFER_SIZE): List[Path] = {
    val sourceFile = new File(source.toString)
    if (!sourceFile.exists) {
      throw new Exception(s"Source file '$sourceFile' does not exist")
    }

    if (!sourceFile.isFile) {
      throw new Exception(s"Source file '$sourceFile' is not a file")
    }

    val fileInputStream = new FileInputStream(source.toString)
    val zipInputStream = new ZipInputStream(fileInputStream)

    val buffer = new Array[Byte](bufferSize)
    var files = ListBuffer[Path]()
    Stream.continually(zipInputStream.getNextEntry).takeWhile(_ != null).foreach { file =>
      if (!file.isDirectory) {
        val outputPath = destination.resolve(file.getName)
        val outputPathParent = outputPath.getParent

        if (!outputPathParent.toFile.exists()) {
          outputPathParent.toFile.mkdirs()
        }

        val fileOutputStream = new FileOutputStream(outputPath.toFile)
        Stream.continually(zipInputStream.read(buffer)).takeWhile(_ != -1).foreach(
          fileOutputStream.write(buffer, 0, _)
        )
        files += outputPath.toAbsolutePath
      }
    }

    files.toList
  }
}
