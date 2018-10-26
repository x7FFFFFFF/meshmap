package jamsesso.meshmap;



import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;


public class Node implements Serializable {
  private final UUID id;
  private  final InetSocketAddress address;

  public Node(InetSocketAddress address) {
    this(UUID.randomUUID(), address);
  }

  public Node(UUID id, InetSocketAddress address) {
    this.id = id;
    this.address = address;
  }

  public int getId() {
    return id.hashCode() & Integer.MAX_VALUE;
  }

  @Override
  public String toString() {
    return address.getHostString() + '#' + address.getPort() + '#' + id;
  }

  public static Node from(String str) {
    if (str == null) {
      throw new IllegalArgumentException("String must not be null");
    }

    String[] parts = str.split("#");

    if (parts.length != 3) {
      throw new IllegalArgumentException("Node address must contain only a host and port");
    }

    String host = parts[0];
    int port;
    UUID id;

    try {
      port = Integer.parseInt(parts[1]);
    }
    catch (NumberFormatException e) {
      throw new IllegalArgumentException("Node address port must be a valid number", e);
    }

    try {
      id = UUID.fromString(parts[2]);
    }
    catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Node ID must be a valid UUID", e);
    }

    return new Node(id, new InetSocketAddress(host, port));
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Node node = (Node) o;
    return Objects.equals(id, node.id) &&
            Objects.equals(address, node.address);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, address);
  }
}
