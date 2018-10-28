package jamsesso.meshmap.client;

import jamsesso.meshmap.MeshMapCluster;
import jamsesso.meshmap.Message;
import jamsesso.meshmap.Node;
import jamsesso.meshmap.Retryable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.System.err;

public class MeshMapClient {
    private final Node self;
    private final MeshMapCluster cluster;

    public MeshMapClient(Node self, MeshMapCluster cluster) {
        this.self = self;
        this.cluster = cluster;
    }

    public  Message message(Node node, Message message) throws IOException {
        try {
            return Retryable.retry(() -> {
                try (Socket socket = new Socket()) {
                    socket.connect(node.getAddress());

                    try (OutputStream outputStream = socket.getOutputStream();
                         InputStream inputStream = socket.getInputStream()) {
                        message.write(outputStream);
                        outputStream.flush();
                        return Message.read(inputStream);
                    }
                }
            }).on(IOException.class).times(3);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public Map<Node, Message> broadcast(Message message) {
        return cluster.getAllNodes().parallelStream()
                .filter(node -> !node.equals(self))
                .map(node -> {
                    try {
                        return new BroadcastResponse(node, message(node, message));
                    } catch (IOException e) {
                        // TODO Better error handling strategy needed.
                        err.println("Unable to broadcast message to node: " + node);
                        e.printStackTrace();

                        return new BroadcastResponse(node, Message.ERR);
                    }
                })
                .collect(Collectors.toMap(BroadcastResponse::getNode, BroadcastResponse::getResponse));
    }
}
