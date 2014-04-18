package org.apache.spark.streaming.dstream

import org.apache.spark.storage.StorageLevel
import scalawebsocket.WebSocket
import org.apache.spark.streaming.StreamingContext
import scala.reflect.ClassTag

object WebSocketInputDStream {
  class WebSocketInputDStream[T: ClassTag](
      @transient ssc_ : StreamingContext,
      url: String,
      bytesToObject: Array[Byte] => T,
      storageLevel: StorageLevel
    ) extends NetworkInputDStream[T](ssc_) {

    def getReceiver(): NetworkReceiver[T] = {
      new WebSocketReceiver(url, bytesToObject, storageLevel)
    }
  }

  class WebSocketReceiver[T: ClassTag](
      url: String,
      bytesToObjects: Array[Byte] => T,
      storageLevel: StorageLevel
    ) extends NetworkReceiver[T] {

    lazy protected val blockGenerator = new BlockGenerator(storageLevel)
    lazy protected val socket = WebSocket().open(url)

    override def getLocationPreference = None

    protected def onStart() {
      blockGenerator.start()
      socket.onTextMessage(m => blockGenerator += bytesToObjects(m.getBytes))
      socket.onBinaryMessage(m => blockGenerator += bytesToObjects(m))
    }

    protected def onStop() {
      blockGenerator.stop()
      socket.shutdown()
    }
  }

  object WebSocketReceiver {
    def bytesToString(bytes: Array[Byte]): String = new String(bytes)
  }

  // Implicitly add webSocket methods to StreamingContext. Can remove if ever added to spark project.
  implicit class WebSocketStreamingContext(val context: StreamingContext) {
    def webSocketStream[T: ClassTag](
        url: String,
        bytesToObjects: Array[Byte] => T,
        storageLevel: StorageLevel
      ): DStream[T] = {
      new WebSocketInputDStream(context, url, bytesToObjects, storageLevel)
    }

    def webSocketTextStream(
        url: String,
        storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
      ): DStream[String] = {
      webSocketStream(url, WebSocketReceiver.bytesToString, storageLevel)
    }
  }
}