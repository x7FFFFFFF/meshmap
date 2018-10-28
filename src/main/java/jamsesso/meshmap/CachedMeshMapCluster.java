package jamsesso.meshmap;

import java.util.NavigableSet;

public class CachedMeshMapCluster implements MeshMapCluster {
    private final Object[] lock = new Object[0];
    private final MeshMapCluster delegate;
    private NavigableSet<Node> nodes;

    public CachedMeshMapCluster(MeshMapCluster cluster) {
        this.delegate = cluster;
    }

    @Override
    public NavigableSet<Node> getAllNodes() {
        synchronized (lock) {
            if (nodes == null) {
                nodes = delegate.getAllNodes();
            }
            return nodes;
        }
    }


    @Override
    public NavigableSet<Node> getAllNodesExcept(Node except) {
        final NavigableSet<Node> allNodes = getAllNodes();
        final NavigableSet<Node> nodesHead = allNodes.headSet(except, false);
        final NavigableSet<Node> nodesTail = allNodes.tailSet(except, false);
        nodesHead.addAll(nodesTail);
        return nodesHead;
    }

    @Override
    public Node getSuccessorNode(Node node) {
        final NavigableSet<Node> allNodes = getAllNodes();
        final Node higher = allNodes.higher(node);
        if (higher == null) {
            return allNodes.first();
        }
        return higher;
    }

    @Override
    public void join(Node node) {
        delegate.join(node);
    }

    public void clearCache() {
        synchronized (lock) {
            nodes = null;
        }
    }
}
