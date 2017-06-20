package tangodeltawhiskey.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

public class TCPSocketChannelEventServer extends
        AbstractEventServer<Collection<SelectionKey>> {

    private static final Logger _LOGGER =
            LoggerFactory.getLogger(TCPSocketChannelEventServer.class);

    private final String _localAddress;
    private final int _port;
    private final ByteBuffer _buffer;
    private final SocketChannelEventHandler _eventHandler;
    private final ExecutorService _eventExecutor;

    private Selector _selector;
    private int _localPort;

    public TCPSocketChannelEventServer(String localAddress, int port, int readBufferSize,
                                       SocketChannelEventHandler eventHandler,
                                       ExecutorService eventExecutor) {

        _localAddress = localAddress;
        _port = port;
        _buffer = ByteBuffer.allocate(readBufferSize);
        _eventHandler = eventHandler;
        _eventExecutor = eventExecutor;
    }

    @Override
    protected Collection<SelectionKey> getEvent() throws Exception {
        _selector.select();

        if (!isRunning()) {
            return Collections.emptyList();
        } else {
            return _selector.selectedKeys();
        }
    }

    @Override
    protected void processEvent(Collection<SelectionKey> selectedKeys) throws Exception {
        Iterator<SelectionKey> it = selectedKeys.iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();

            if (!key.isValid()) {
                continue;
            }

            if (key.isAcceptable()) {
                ServerSocketChannel serverSocketChannel =
                        (ServerSocketChannel) key.channel();

                SocketChannel socketChannel
                        = serverSocketChannel.accept();

                Socket socket = socketChannel.socket();
                _LOGGER.debug("Accepted connection from {}:{}",
                        socket.getInetAddress(), socket.getPort());

                socketChannel.configureBlocking(false);
                socketChannel.register(_selector, SelectionKey.OP_READ);

                onAccept(socketChannel);

            } else if (key.isReadable()) {
                SocketChannel socketChannel = (SocketChannel) key.channel();
                Socket socket = socketChannel.socket();

                _buffer.clear();

                try {
                    int count = socketChannel.read(_buffer);
                    if (count > 0) {
                        _LOGGER.debug("Read {} bytes from {}:{}", count,
                                socket.getInetAddress(), socket.getPort());

                        onRead(socketChannel,
                                Arrays.copyOf(_buffer.array(), count));
                    } else if (count == -1) {
                        _LOGGER.debug("Connection disconnected from {}:{} ",
                                socket.getInetAddress(), socket.getPort());

                        onDisconnect(socketChannel);

                        try {
                            key.channel().close();
                            key.cancel();
                        } catch (IOException ex) {
                            _LOGGER.warn("Failure while attempting to clean up " +
                                            "channel resources used by {}:{}",
                                    socket.getInetAddress(), socket.getPort(), ex);
                        }
                    }

                } catch (IOException ex) {
                    _LOGGER.error("Error while reading from {}:{}",
                            socket.getInetAddress(), socket.getPort(), ex);

                    _LOGGER.debug("Closing connection from {}:{}",
                            socket.getInetAddress(), socket.getPort(), ex);
                    key.channel().close();
                    key.cancel();
                }
            }
        }
    }

    @Override
    protected void doStart() throws Exception {

        _selector = SelectorProvider.provider().openSelector();

        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);

        ServerSocket socket = serverSocketChannel.bind(
                new InetSocketAddress(_localAddress, _port)).socket();

        _localPort = socket.getLocalPort();

        _LOGGER.debug("Server is now bound to {}:{}",
                socket.getInetAddress(), _localPort);

        serverSocketChannel.register(_selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    protected void doStop() throws Exception {
        _selector.close();
    }

    @Override
    protected void doInterrupt() {
        _selector.wakeup();
    }

    @Override
    protected void afterRunning() {
        _eventExecutor.submit(() -> _eventHandler.onStart(_localPort));
    }

    @Override
    protected void afterDone() {
        _eventExecutor.submit(() -> _eventHandler.onStop());
    }

    @Override
    protected void onFailure(State state, Exception e) {
        _eventExecutor.submit(() -> _eventHandler.onFailure(e));
    }

    protected void onAccept(SocketChannel socketChannel) {
        _eventExecutor.submit(() -> _eventHandler.onAccept(socketChannel));
    }

    protected void onRead(SocketChannel socketChannel, byte[] bytes) {
        _eventExecutor.submit(() -> _eventHandler.onRead(socketChannel, bytes));
    }

    protected void onDisconnect(SocketChannel socketChannel) {
        _eventExecutor.submit(() -> _eventHandler.onDisconnect(socketChannel));
    }
}
