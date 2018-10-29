package jamsesso.meshmap;

import java.io.File;
import java.util.NavigableSet;

public class CachedMeshMapCluster extends LocalMeshMapCluster {
    private final Object[] lock = new Object[0];

    private NavigableSet<Node> nodes;

    public CachedMeshMapCluster(File directory) {
        super(directory);
    }

    @Override
    public NavigableSet<Node> getAllNodes() {
        synchronized (lock) {
            if (nodes == null) {
                nodes = super.getAllNodes();
            }
            return nodes;
        }
    }

    public void clearCache() {
        synchronized (lock) {
            nodes = null;
        }
    }
}
