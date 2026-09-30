import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/** Standalone Android socket regression probe; uses only ephemeral loopback ports. */
public final class PortReuseProbe {
    public static void main(String[] args) throws Exception {
        probe(false);
        probe(true);
    }

    private static void probe(boolean reuse) throws Exception {
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        int port;
        try (ServerSocket listener = new ServerSocket()) {
            listener.setReuseAddress(reuse);
            listener.bind(new InetSocketAddress(loopback, 0));
            port = listener.getLocalPort();
            try (ServerSocket duplicate = new ServerSocket()) {
                duplicate.setReuseAddress(reuse);
                try {
                    duplicate.bind(new InetSocketAddress(loopback, port));
                    throw new AssertionError("A live listener must remain exclusive");
                } catch (BindException expected) {
                    System.out.println("reuse=" + reuse + " live listener: conflict correctly rejected");
                }
            }
            try (Socket client = new Socket(loopback, port);
                 Socket accepted = listener.accept()) {
                client.setSoTimeout(5000);
                accepted.getOutputStream().write(42);
                if (client.getInputStream().read() != 42) throw new AssertionError("No data");
                // Server initiates graceful close so its local port enters TIME_WAIT.
                accepted.close();
                if (client.getInputStream().read() != -1) throw new AssertionError("No EOF");
            }
        }
        try (ServerSocket replacement = new ServerSocket()) {
            replacement.setReuseAddress(reuse);
            try {
                replacement.bind(new InetSocketAddress(loopback, port));
                System.out.println("reuse=" + reuse + " immediate rebind: OK");
            } catch (BindException failure) {
                System.out.println("reuse=" + reuse + " immediate rebind: " + failure);
                if (reuse) throw failure;
            }
        }
    }
}
