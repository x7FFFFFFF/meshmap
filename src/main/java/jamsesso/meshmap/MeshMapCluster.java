package jamsesso.meshmap;

import java.util.NavigableSet;

public interface MeshMapCluster {
    NavigableSet<Node> getAllNodes();

    NavigableSet<Node> getAllNodesExcept(Node except);

    Node getSuccessorNode(Node node);

    Node getNodeForKey(Object key);

    void join(Node node);
}
