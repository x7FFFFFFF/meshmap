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
    public NavigableSet<Node> getAllNodesExcept(Node except) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node getSuccessorNode(Node node) {
        throw new UnsupportedOperationException();
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
}
