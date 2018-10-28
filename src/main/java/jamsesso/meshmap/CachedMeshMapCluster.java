package jamsesso.meshmap;

import java.util.List;

public class CachedMeshMapCluster implements MeshMapCluster {
  private final Object[] lock = new Object[0];
  private final MeshMapCluster delegate;
  private List<Node> nodes;

  public CachedMeshMapCluster(MeshMapCluster cluster) {
    this.delegate = cluster;
  }

  @Override
  public List<Node> getAllNodes() {
    synchronized (lock) {
      if(nodes == null) {
        nodes = delegate.getAllNodes();
      }

      return nodes;
    }
  }

  @Override
  public void join(Node node) throws MeshMapException {
     delegate.join(node);
  }

  public void clearCache() {
    synchronized (lock) {
      nodes = null;
    }
  }
}
