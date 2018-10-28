package jamsesso.meshmap.server;


import jamsesso.meshmap.*;
import jamsesso.meshmap.examples.InvocationContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;




public class MeshMapServer implements Runnable, AutoCloseable {
    private final MessageHandler messageHandler;
    private final Node self;
    private final ExecutorService service;
    private ServerSocket serverSocket;

    public MeshMapServer(Node self, MessageHandler messageHandler, ExecutorService service) {
        this.self = self;
        this.service = service;
        if (messageHandler == null) {
            throw new IllegalStateException("Cannot restart a dead mesh map server");
        }
        this.messageHandler = messageHandler;
    }


    public Future start() {
        if (InvocationContext.get().isSyncMode()) {
            run();
        } else {
            return service.submit(this);
        }
        return null;
    }


    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(self.getAddress().getPort());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        while (!serverSocket.isClosed()) {
            try (Socket socket = serverSocket.accept();
                 InputStream inputStream = socket.getInputStream();
                 OutputStream outputStream = socket.getOutputStream()) {
                Message message = Message.read(inputStream);
                Message response = messageHandler.handle(message);

                if (response == null) {
                    response = Message.ACK;
                }

                response.write(outputStream);
                outputStream.flush();
            } catch (SocketException e) {
                // Socket was closed. Nothing to do here. Node is going down.
            } catch (IOException e) {
                throw new RuntimeException(e);
                // TODO Better error handling strategy is needed.
                //err.println("Unable to accept connection");
                //e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {
        serverSocket.close();
        System.out.println("closed:" + self);
    }


}
