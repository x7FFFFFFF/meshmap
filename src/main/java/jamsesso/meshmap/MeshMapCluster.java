package jamsesso.meshmap;

import java.util.List;

public interface MeshMapCluster {
    List<Node> getAllNodes();

    void join(Node node) throws MeshMapException;
}
