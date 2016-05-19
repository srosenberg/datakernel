package io.datakernel.eventloop;

public interface AsyncUdpSocket {
	interface EventHandler {
		void onRead(UdpPacket packet);

		void onSent();

		void onClosedWithError(Exception e);

		void onRegistered();
	}

	void setEventHandler(AsyncUdpSocket.EventHandler eventHandler);

	void read();

	void send(UdpPacket packet);

	void close();
}
