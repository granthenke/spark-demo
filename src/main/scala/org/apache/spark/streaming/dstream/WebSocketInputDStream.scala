package org.apache.spark.streaming.dstream

import org.apache.spark.storage.StorageLevel
import scalawebsocket.WebSocket
import org.apache.spark.streaming.StreamingContext
import scala.reflect.ClassTag

object WebSocketInputDStream {
  class WebSocketInputDStream[T: ClassTag](
      @transient ssc_ : StreamingContext,
      url: String,
      textMessageHandler: String => Option[T],
      binaryMessageHandler: Array[Byte] => Option[T],
      storageLevel: StorageLevel
    ) extends NetworkInputDStream[T](ssc_) {

    def getReceiver(): NetworkReceiver[T] = {
      new WebSocketReceiver(url, textMessageHandler, binaryMessageHandler, storageLevel)
    }
  }

  class WebSocketReceiver[T: ClassTag](
      url: String,
      textMessageHandler: String => Option[T],
      binaryMessageHandler: Array[Byte] => Option[T],
      storageLevel: StorageLevel
    ) extends NetworkReceiver[T] {

    lazy protected val blockGenerator = new BlockGenerator(storageLevel)
    lazy protected val socket = WebSocket()

    override def getLocationPreference = None

    protected def onStart() {
      logInfo("Connecting to: " + url)
      socket.open(url)
      logInfo("Connected to: " + url)

      blockGenerator.start()
      socket.onTextMessage(m => textMessageHandler(m).map(blockGenerator += _))
      socket.onBinaryMessage(m => binaryMessageHandler(m).map(blockGenerator += _))
    }

    protected def onStop() {
      socket.shutdown()
      blockGenerator.stop()
    }
  }

  object WebSocketReceiver {
    def wrap(s: String) = Option(s)
    def none(a: Any) = None
  }

  // Implicitly add webSocket methods to StreamingContext. Can remove if ever added to spark project.
  implicit class WebSocketStreamingContext(val context: StreamingContext) {
    def webSocketStream[T: ClassTag](
        url: String,
        textMessageHandler: String => Option[T],
        binaryMessageHandler: Array[Byte] => Option[T],
        storageLevel: StorageLevel
      ): DStream[T] = {
      new WebSocketInputDStream(context, url, textMessageHandler, binaryMessageHandler, storageLevel)
    }

    def webSocketTextStream(
        url: String,
        storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
      ): DStream[String] = {
      webSocketStream(url, WebSocketReceiver.wrap, WebSocketReceiver.none, storageLevel)
    }
  }
}