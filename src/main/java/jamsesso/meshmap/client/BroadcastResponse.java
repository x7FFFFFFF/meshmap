package jamsesso.meshmap.client;

import jamsesso.meshmap.Message;
import jamsesso.meshmap.Node;

public class BroadcastResponse {
    final Node node;
    final Message response;

    BroadcastResponse(Node node, Message response) {
        this.node = node;
        this.response = response;
    }

    Node getNode() {
        return node;
    }

    Message getResponse() {
        return response;
    }
}
