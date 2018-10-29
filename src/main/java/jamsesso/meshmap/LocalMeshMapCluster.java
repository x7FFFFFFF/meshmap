package jamsesso.meshmap;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

public class LocalMeshMapCluster implements MeshMapCluster, AutoCloseable {
    private final File directory;

    public LocalMeshMapCluster(File directory) {
        directory.mkdirs();

        if (!directory.isDirectory()) {
            throw new IllegalArgumentException("File passed to LocalMeshMapCluster must be a directory");
        }

        if (!directory.canRead() || !directory.canWrite()) {
            throw new IllegalArgumentException("Directory must be readable and writable");
        }

        this.directory = directory;
    }

    @Override
    public NavigableSet<Node> getAllNodes() {
        final NavigableSet<Node> nodes = new TreeSet<>(Comparator.comparingInt(Node::getId));
        Stream.of(Objects.requireNonNull(directory.listFiles()))
                .filter(File::isFile)
                .map(File::getName)
                .map(Node::from).forEach(nodes::add);
        return nodes;
    }



    @Override
    public Node getNodeForKey(Object key) {
        /*
        *    private Node getNodeForKey(Object key) {
        int hash = key.hashCode() & Integer.MAX_VALUE;
        List<Node> nodes = cluster.getAllNodes();

        for (Node node : nodes) {
            if (hash <= node.getId()) {
                return node;
            }
        }

        return nodes.get(0);
    }*/
        final NavigableSet<Node> allNodes = getAllNodes();
        int hash = key.hashCode() & Integer.MAX_VALUE;
        Node node = new Node(hash);
        final Node higher = allNodes.ceiling(node);
        return (higher != null) ? higher : allNodes.first();
    }

    @Override
    public void join(Node node) {
        File file = new File(directory.getAbsolutePath() + File.separator + node.toString());
        try {
            boolean didCreateFile = file.createNewFile();

            if (!didCreateFile) {
                throw new RuntimeException("File could not be created: " + file.getName());
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to join cluster", e);
        }

        file.deleteOnExit();

    /*server = new MeshMapServer(this, self);
    MeshMapImpl<K, V> map = new MeshMapImpl<>(this, server, self);

    try {
      server.start(map);
      map.open();
    }
    catch(IOException e) {
      throw new MeshMapException("Unable to start the mesh map server", e);
    }

    server.broadcast(Message.HI);
    this.map = map;

    return map;*/
    }

    @Override
    public void close() throws Exception {
   /* File file = new File(directory.getAbsolutePath() + File.separator + self.toString());
    boolean didDeleteFile = file.delete();

    if (!didDeleteFile) {
      throw new MeshMapException("File could not be deleted: " + file.getName());
    }*/

 /*   if (server != null) {
      server.broadcast(Message.BYE);
      server.close();
    }*/
    }

    @Override
    public NavigableSet<Node> getAllNodesExcept(Node except) {
        final NavigableSet<Node> allNodes = getAllNodes();
        final NavigableSet<Node> nodesHead = allNodes.headSet(except, false);
        final NavigableSet<Node> nodesTail = allNodes.tailSet(except, false);
        nodesHead.addAll(nodesTail);
        return nodesHead;
    }
/*
*
    private Node getSuccessorNode() {
        List<Node> nodes = cluster.getAllNodes();

        if (nodes.size() <= 1) {
            return null;
        }

        int selfIndex = Collections.binarySearch(nodes, self, Comparator.comparingInt(Node::getId));
        int successorIndex = selfIndex + 1;

        // Find the successor node.
        if (successorIndex > nodes.size() - 1) {
            return nodes.get(0);
        } else {
            return nodes.get(successorIndex);
        }
    }
* */
    @Override
    public Node getSuccessorNode(Node node) {
        final NavigableSet<Node> allNodes = getAllNodes();
        final Node higher = allNodes.higher(node);
        if (higher == null) {
            return allNodes.first();
        }
        return higher;
    }
}
