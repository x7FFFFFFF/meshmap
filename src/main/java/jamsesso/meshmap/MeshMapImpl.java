package jamsesso.meshmap;


import jamsesso.meshmap.client.MeshMapClient;
import jamsesso.meshmap.server.MeshMapServer;

import java.io.IOException;
import java.io.Serializable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static jamsesso.meshmap.server.MessageHandlerImpl.*;

public class MeshMapImpl<K, V> implements MeshMap<K, V> {


    private final CachedMeshMapCluster cluster;

    private final MeshMapClient client;
    private final Node self;
    private final Map<Object, Object> delegate;

    public MeshMapImpl(MeshMapCluster cluster, MeshMapClient client, Node self) {
        this.cluster = new CachedMeshMapCluster(cluster);
        this.client = client;
        this.self = self;
        this.delegate = new ConcurrentHashMap<>();
    }



    @Override
    public int size() {
        Message sizeMsg = new Message(TYPE_SIZE);

        return delegate.size() + client.broadcast(sizeMsg).entrySet().stream()
                .map(Map.Entry::getValue)
                .filter(response -> TYPE_SIZE.equals(response.getType()))
                .mapToInt(Message::getPayloadAsInt)
                .sum();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        Node target = getNodeForKey(key);

        if (target.equals(self)) {
            // Key lives on the current node.
            return delegate.containsKey(key);
        }

        Message containsKeyMsg = new Message(TYPE_CONTAINS_KEY, key);
        Message response;

        try {
            response = client.message(target, containsKeyMsg);
        } catch (IOException e) {
            throw new MeshMapRuntimeException(e);
        }

        return Message.YES.equals(response);
    }

    @Override
    public boolean containsValue(Object value) {
        if (delegate.containsValue(value)) {
            // Check locally first.
            return true;
        }

        Message containsValueMsg = new Message(TYPE_CONTAINS_VALUE, value);

        return client.broadcast(containsValueMsg).entrySet().stream()
                .map(Map.Entry::getValue)
                .anyMatch(Message.YES::equals);
    }

    @Override
    public V get(Object key) {
        return (V) get(key, getNodeForKey(key));
    }

    @Override
    public V put(K key, V value) {
        put(key, value, getNodeForKey(key));
        return value;
    }

    @Override
    public V remove(Object key) {
        return (V) remove(key, getNodeForKey(key));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        m.entrySet().parallelStream().forEach(entry -> put(entry.getKey(), entry.getValue()));
    }

    @Override
    public void clear() {
        Message clearMsg = new Message(TYPE_CLEAR);
        client.broadcast(clearMsg);
        delegate.clear();
    }

    @Override
    public Set<K> keySet() {
        return cluster.getAllNodes().parallelStream()
                .map(this::keySet)
                .flatMap(Stream::of)
                .map(object -> (K) object)
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<V> values() {
        return entrySet().stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        Message dumpEntriesMsg = new Message(TYPE_DUMP_ENTRIES);
        Set<Map.Entry<K, V>> entries = new HashSet<>();

        for (Map.Entry<Object, Object> localEntry : delegate.entrySet()) {
            entries.add(new TypedEntry<>((K) localEntry.getKey(), (V) localEntry.getValue()));
        }

        for (Map.Entry<Node, Message> response : client.broadcast(dumpEntriesMsg).entrySet()) {
            Entry[] remoteEntries = response.getValue().getPayload(Entry[].class);

            for (Entry remoteEntry : remoteEntries) {
                entries.add(new TypedEntry<>((K) remoteEntry.getKey(), (V) remoteEntry.getValue()));
            }
        }

        return entries;
    }

    @Override
    public String toString() {
        return "MeshMapImpl(Local)[" + String.join(", ", delegate.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue()).toArray(String[]::new)) + "]";
    }

    public void open() {
        Node successor = getSuccessorNode();

        // If there is no successor, there is nothing to do.
        if (successor == null) {
            return;
        }

        // Ask the successor for their key set.
        Object[] keySet = keySet(successor);

        // Transfer the keys from the successor node that should live on this node.
        List<Object> keysToTransfer = Stream.of(keySet)
                .filter(key -> {
                    int hash = key.hashCode() & Integer.MAX_VALUE;

                    if (self.getId() > successor.getId()) {
                        // The successor is the first node (circular node list)
                        return hash <= self.getId() && hash > successor.getId();
                    }

                    return hash <= self.getId();
                })
                .collect(Collectors.toList());

        // Store the values on the current node.
        keysToTransfer.forEach(key -> delegate.put(key, get(key, successor)));

        // Delete the keys from the remote node now that the keys are transferred.
        keysToTransfer.forEach(key -> remove(key, successor));
    }

    @Override
    public void close() {
        Node successor = getSuccessorNode();

        // If there is no successor, there is nothing to do.
        if (successor == null) {
            return;
        }

        // Transfer the data from this node to the successor node.
        delegate.forEach((key, value) -> put(key, value, successor));
    }

    private Node getNodeForKey(Object key) {
        int hash = key.hashCode() & Integer.MAX_VALUE;
        List<Node> nodes = cluster.getAllNodes();

        for (Node node : nodes) {
            if (hash <= node.getId()) {
                return node;
            }
        }

        return nodes.get(0);
    }

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

    private Object get(Object key, Node target) {
        if (target.equals(self)) {
            // Value is stored on the local server.
            return delegate.get(key);
        }

        Message getMsg = new Message(TYPE_GET, key);
        Message response;

        try {
            response = client.message(target, getMsg);
        } catch (IOException e) {
            throw new MeshMapRuntimeException(e);
        }

        if (!TYPE_GET.equals(response.getType())) {
            throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
        }

        return response.getPayload(Object.class);
    }

    private Object put(Object key, Object value, Node target) {
        if (target.equals(self)) {
            // Value is stored on the local server.
            return delegate.put(key, value);
        }

        Message putMsg = new Message(TYPE_PUT, new Entry(key, value));
        Message response;

        try {
            response = client.message(target, putMsg);
        } catch (IOException e) {
            throw new MeshMapRuntimeException(e);
        }

        if (!Message.ACK.equals(response)) {
            throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
        }

        return value;
    }

    private Object remove(Object key, Node target) {
        if (target.equals(self)) {
            // Value is stored on the local server.
            return delegate.remove(key);
        }

        Message removeMsg = new Message(TYPE_REMOVE, key);
        Message response;

        try {
            response = client.message(target, removeMsg);
        } catch (IOException e) {
            throw new MeshMapRuntimeException(e);
        }

        if (!TYPE_REMOVE.equals(response.getType())) {
            throw new MeshMapRuntimeException("Unexpected response from remote node: " + response);
        }

        return response.getPayload(Object.class);
    }

    private Object[] keySet(Node target) {
        if (target.equals(self)) {
            // Key is on local server.
            return delegate.keySet().toArray();
        }

        Message keySetMsg = new Message(TYPE_KEY_SET);

        try {
            Message response = client.message(target, keySetMsg);
            return response.getPayload(Object[].class);
        } catch (IOException e) {
            throw new MeshMapRuntimeException(e);
        }
    }


    public static class Entry implements Serializable {
        final Object key;
        final Object value;

        public Entry(Object key, Object value) {
            this.key = key;
            this.value = value;
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }
    }


    private static class TypedEntry<K, V> implements Map.Entry<K, V> {
        final K key;
        final V value;

        TypedEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }
    }
}
