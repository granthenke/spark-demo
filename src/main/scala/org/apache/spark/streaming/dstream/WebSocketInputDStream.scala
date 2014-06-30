package org.apache.spark.streaming.dstream

import org.apache.spark.storage.StorageLevel
import scalawebsocket.WebSocket
import org.apache.spark.streaming.StreamingContext
import scala.reflect.ClassTag
import org.apache.spark.streaming.receiver.{Receiver, BlockGenerator}
import org.apache.spark.Logging
import java.net.Socket
import scala.collection.mutable
import scala.collection.immutable.Queue
import akka.actor.IO.Iteratee

object WebSocketInputDStream {
  class WebSocketInputDStream[T: ClassTag](
      @transient ssc_ : StreamingContext,
      url: String,
      textMessageHandler: String => Option[T],
      binaryMessageHandler: Array[Byte] => Option[T],
      storageLevel: StorageLevel
    ) extends ReceiverInputDStream[T](ssc_) {

    def getReceiver(): Receiver[T] = {
      new WebSocketReceiver(url, textMessageHandler, binaryMessageHandler, storageLevel)
    }
  }

  class WebSocketReceiver[T: ClassTag](
      url: String,
      textMessageHandler: String => Option[T],
      binaryMessageHandler: Array[Byte] => Option[T],
      storageLevel: StorageLevel
    ) extends Receiver[T](storageLevel) with Logging {

    private var webSocket: WebSocket = _

    def onStart() {
      try{
        logInfo("Connecting to: " + url)
        val newWebSocket = WebSocket()
        newWebSocket.onTextMessage(m => textMessageHandler(m).map(store))
        newWebSocket.onBinaryMessage(m => binaryMessageHandler(m).map(store))
        // socket.onError() TODO(GH): What to do here? restart?
        newWebSocket.open(url)
        setWebSocket(newWebSocket)
        logInfo("Connected to: " + url)
    } catch {
      case e: Exception => restart("Error starting WebSocket stream", e)
    }
    }

    def onStop() {
      setWebSocket(null)
      logInfo("WebSocket receiver stopped")
    }

    private def setWebSocket(newWebSocket: WebSocket) = synchronized {
      if (webSocket != null) {
        webSocket.shutdown()
      }
      webSocket = newWebSocket
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