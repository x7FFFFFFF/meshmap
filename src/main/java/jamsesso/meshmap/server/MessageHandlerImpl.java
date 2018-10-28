package jamsesso.meshmap.server;

import jamsesso.meshmap.CachedMeshMapCluster;
import jamsesso.meshmap.MeshMapImpl;
import jamsesso.meshmap.Message;
import jamsesso.meshmap.MessageHandler;

import java.nio.ByteBuffer;

public class MessageHandlerImpl implements MessageHandler {
    public static final String TYPE_PUT = "PUT";
    public static final String TYPE_GET = "GET";
    public static final String TYPE_REMOVE = "REMOVE";
    public static final String TYPE_CLEAR = "CLEAR";
    public static final String TYPE_KEY_SET = "KEY_SET";
    public static final String TYPE_SIZE = "SIZE";
    public static final String TYPE_CONTAINS_KEY = "CONTAINS_KEY";
    public static final String TYPE_CONTAINS_VALUE = "CONTAINS_VALUE";
    public static final String TYPE_DUMP_ENTRIES = "DUMP_ENTRIES";
    private final MeshMapImpl<Object, Object> delegate;
    private final CachedMeshMapCluster cluster;

    public MessageHandlerImpl(MeshMapImpl<Object, Object> delegate, CachedMeshMapCluster cluster) {
        this.delegate = delegate;
        this.cluster = cluster;
    }

    @Override
    public Message handle(Message message) {
        switch (message.getType()) {
            case Message.TYPE_HI:
            case Message.TYPE_BYE: {
                cluster.clearCache();
                return Message.ACK;
            }

            case TYPE_GET: {
                Object key = message.getPayload(Object.class);
                return new Message(TYPE_GET, delegate.get(key));
            }

            case TYPE_PUT: {
                MeshMapImpl.Entry entry = message.getPayload(MeshMapImpl.Entry.class);
                delegate.put(entry.getKey(), entry.getValue());
                return Message.ACK;
            }

            case TYPE_REMOVE: {
                Object key = message.getPayload(Object.class);
                return new Message(TYPE_REMOVE, delegate.remove(key));
            }

            case TYPE_CLEAR: {
                delegate.clear();
                return Message.ACK;
            }

            case TYPE_KEY_SET: {
                Object[] keys = delegate.keySet().toArray();
                return new Message(TYPE_KEY_SET, keys);
            }

            case TYPE_SIZE: {
                return new Message(TYPE_SIZE, ByteBuffer.allocate(4).putInt(delegate.size()).array());
            }

            case TYPE_CONTAINS_KEY: {
                Object key = message.getPayload(Object.class);
                return delegate.containsKey(key) ? Message.YES : Message.NO;
            }

            case TYPE_CONTAINS_VALUE: {
                Object value = message.getPayload(Object.class);
                return delegate.containsValue(value) ? Message.YES : Message.NO;
            }

            case TYPE_DUMP_ENTRIES: {
                MeshMapImpl.Entry[] entries = delegate.entrySet().stream()
                        .map(entry -> new MeshMapImpl.Entry(entry.getKey(), entry.getValue())).toArray(MeshMapImpl.Entry[]::new);

                return new Message(TYPE_DUMP_ENTRIES, entries);
            }

            default: {
                return Message.ACK;
            }
        }
    }
}
