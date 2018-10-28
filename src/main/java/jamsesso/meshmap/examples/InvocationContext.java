package jamsesso.meshmap.examples;

public class InvocationContext {
    private final boolean forceClose;
    private final boolean syncMode;
    private static final ThreadLocal<InvocationContext> threadLocal = new ThreadLocal<>();
    public static void set(InvocationContext context) {
        threadLocal.set(context);
    }
    public static InvocationContext get() {
        return threadLocal.get();
    }


    InvocationContext(boolean forceClose, boolean syncMode) {
        this.forceClose = forceClose;
        this.syncMode = syncMode;
    }

    public boolean isForceClose() {
        return forceClose;
    }

    public boolean isSyncMode() {
        return syncMode;
    }


    public  static class Builder {
        private boolean forceClose;
        private boolean syncMode;

        public Builder setForceClose(boolean forceClose) {
            this.forceClose = forceClose;
            return this;
        }

        public Builder setSyncMode(boolean syncMode) {
            this.syncMode = syncMode;
            return this;
        }

        public InvocationContext build() {
            return new InvocationContext(forceClose, syncMode);
        }
    }
}
