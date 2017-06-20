package tangodeltawhiskey.async;

import java.nio.channels.SocketChannel;

public interface SocketChannelEventHandler {

    void onAccept(SocketChannel socketChannel);

    void onDisconnect(SocketChannel socketChannel);

    void onRead(SocketChannel socketChannel, byte[] bytes);

    void onStart(int port);

    void onStop();

    void onFailure(Exception e);
}
