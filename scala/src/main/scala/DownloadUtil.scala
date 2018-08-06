import java.net.URL
import java.nio.channels.Channels
import java.nio.file.Path
import java.io.FileOutputStream

object DownloadUtil {
  val DEFAULT_MAX_FILE_SIZE: Int = 268435456 // 2^28

  /**
    * Download a file from a url into a destination path
    */
  def download(source: URL, destination: Path, maxByteCount: Int = DEFAULT_MAX_FILE_SIZE): Unit = {
    val inputChannel = Channels.newChannel(
      source.openStream
    )
    val outputChannel = new FileOutputStream(destination.toString).getChannel

    outputChannel.transferFrom(inputChannel, 0, maxByteCount)
  }
}
