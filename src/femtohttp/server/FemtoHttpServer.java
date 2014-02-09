package femtohttp.server;



import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.servlet.ServletHandler;

public class FemtoHttpServer {

    private static String uri = "/femtoserver";
    private static int port = -8080;
    private static int maxThreads = 120;
    
    public static void main(String[] args) {
        try {
           FemtoHttpServer femtoHttpServer = new FemtoHttpServer();
           femtoHttpServer.startServer(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startServer(String[] args) throws Exception {
    
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMaxThreads(FemtoHttpServer.maxThreads);
        Server server = new Server(threadPool);

        ServletHandler handler = new ServletHandler();
        server.setHandler(handler);
        handler.addServletWithMapping(femtohttp.server.FemtoDBConnector.class, "/femtoserver");

        ServerConnector http = new ServerConnector(server);
        http.setPort(8080);
        http.setIdleTimeout(30000);
        server.addConnector(http);
        server.start();
        server.join();
    }


}