package jamsesso.meshmap.examples;

import jamsesso.meshmap.LocalMeshMapCluster;
import jamsesso.meshmap.MeshMap;
import jamsesso.meshmap.MeshMapException;
import jamsesso.meshmap.Node;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


import static java.lang.System.in;
import static java.lang.System.out;

public class InteractiveNode implements AutoCloseable {
    private final Scanner scanner;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Node self;
    private final LocalMeshMapCluster cluster;
    private final MeshMap<String, String> map;
    private final int selfPort;
    private volatile boolean stopped = false;
    private final String directory;
    private final Map<String, Command> STRING_COMMAND_MAP = new TreeMap<>();
    private final Map<String, Future> futureMap = new ConcurrentHashMap<>();

    {
        add(new Command("add", "add a key/value pair", this::addPair));
        add(new Command("get", "get a value", this::getValue));
        add(new Command("rm", "remove a value", this::removeValue));
        add(new Command("size", "get map size", this::getSize));
        add(new Command("keys", "get all keys", this::getKeys));
        add(new Command("q", "quit", this::quit));
        add(new Command("inf", "current port and cluster", this::printSettings));
        add(new Command("list", "nodes list", this::printNodesList));
        add(new Command("node", "add node", this::addNode));
        add(new Command("rmnode", "remove node", this::removeNode));
        add(new Command("h", "help", this::help));
    }

    private void quit() {
        executorService.shutdownNow();
        stopped = true;
    }

    private void help() {
        STRING_COMMAND_MAP.values().forEach(out::println);
    }


    private InteractiveNode(Scanner scanner, int selfPort, String directory) throws MeshMapException {
        this.scanner = scanner;
        this.selfPort = selfPort;
        this.directory = directory;
        this.cluster = new LocalMeshMapCluster( new File("cluster/" + directory));
        this.self = new Node(new InetSocketAddress("127.0.0.1", selfPort));
        cluster.join(this.self);

        runNodeThread(this.self);

     /*
        this.cluster = new LocalMeshMapCluster(self, new File("cluster/" + directory));
        this.map = cluster.join();*/
    }


    @Override
    public void close() throws Exception {
        map.close();
        cluster.close();
    }

    public static void main(String[] args) throws Exception {
        out.println("Print 'h' for help.");
        int port;
        String directory;
        if (args.length == 2) {
            port = Integer.parseInt(args[0]);
            directory = args[1];
        } else {
            port = 8844;
            directory = "cluster1";
        }
        final Scanner scanner = new Scanner(in);

        try (InteractiveNode interactiveNode = new InteractiveNode(scanner, port, directory)) {
            while (!interactiveNode.stopped) {
                final String command = scanner.nextLine();
                interactiveNode.execute(command);
            }
        }
    }

    private void removeNode() {
        out.print("Key: ");
        String key = scanner.nextLine();
        if (self.toString().equals(key)) {
            out.print("Cant remove interactive node. Use 'q' command to quit all.");
        }

        final Future future = futureMap.get(key);
        future.cancel(true);
    }

    private void addNode() {
        out.print("Port: ");
        int port = Integer.parseInt(scanner.nextLine());
        Node node = new Node(new InetSocketAddress("127.0.0.1", port));
        try {
            cluster.join(node);
        } catch (MeshMapException e) {
            throw new RuntimeException(e);
        }
        runNodeThread(node);
    }

    private void runNodeThread(Node node) {
        //Node node = new Node(new InetSocketAddress("127.0.0.1", port));
        futureMap.put(node.toString(), executorService.submit(() -> {
            InvocationContext.set(new InvocationContext.Builder().setSyncMode(true).build());

            try (LocalMeshMapCluster cluster = new LocalMeshMapCluster( new File("cluster/" + directory));


                 MeshMap<String, String> map = cluster.join()) {
                out.println("node started:" + node);
                while (!Thread.interrupted()) {
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                out.println("Interrupted node=" + node);
                if (!(e instanceof InterruptedException)) {
                    e.printStackTrace();
                }
            }

        }));
    }

    private void printNodesList() {
        cluster.getAllNodes().forEach(out::println);
    }

    private void printSettings() {
        out.println("self node = " + self);
    }

    private void addPair() {
        out.print("Key: ");
        String key = scanner.nextLine();
        out.print("Value: ");
        String value = scanner.nextLine();

        map.put(key, value);
        out.println("OK");
    }

    private void getValue() {
        out.print("Search for key: ");
        String key = scanner.nextLine();
        String value = map.get(key);

        if (value == null) {
            out.println("(not found)");
        } else {
            out.println(value);
        }
    }

    private void getKeys() {
        out.println("All keys: " + Arrays.toString(map.keySet().toArray()));
    }

    private void getSize() {
        out.println("Size: " + map.size());
    }

    private void removeValue() {
        out.print("Key to remove: ");
        String key = scanner.nextLine();
        map.remove(key);
        out.println("OK");
    }


    private void add(Command command) {
        STRING_COMMAND_MAP.put(command.command, command);
    }

    private void execute(String command) {
        final Command cmd = STRING_COMMAND_MAP.get(command);
        if (cmd == null) {
            out.printf("Command '%s' not found. %n", command);
            out.flush();
            return;
        }
        try {
            cmd.action.execute();
        } catch (Exception e) {
            out.printf("Execution error for command '%s' %n", command);
            out.flush();
            e.printStackTrace();
        }
    }


    private static class Command {
        private final String command;
        private final String descr;
        private final ICommand action;

        Command(String command, String descr, ICommand action) {
            this.command = command;
            this.descr = descr;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Command{" +
                    "command='" + command + '\'' +
                    ", descr='" + descr + '\'' +
                    ", action=" + action +
                    '}';
        }
    }

    @FunctionalInterface
    interface ICommand {
        void execute() throws Exception;
    }

}
