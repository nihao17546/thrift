package test.thrift.server;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.junit.Test;
import test.thrift.api.TService;
import test.thrift.impl.TServiceImpl;

/**
 * Created by nihao on 16/11/27.
 */
public class Server {
    private final int simple_port=8001;
    private final int port02=8002;
    private final int port03=8003;
    private final int port04=8003;

    /**
     * TSimpleServer
     * 简单的单线程服务模型
     * 说明:
     * TSimpleServer接收一个连接，处理这个连接上的请求直到client关闭该连接，
     * 才去重新接受一个新连接。因为所有事情都在一个线程且是阻塞I/O，
     * 它仅能同时服务一个连接，其他client不得不等待直到被接收。
     * TSimpleServer主要用于测试目的，不要在生产环境中使用。
     * @throws Exception
     */
    @Test
    public void testSimple() throws Exception {
        System.out.println("simple server start ...");
        TProcessor tProcessor=new TService.Processor<TService.Iface>(new TServiceImpl());
        TServerSocket serverSocket=new TServerSocket(simple_port);
        TServer.Args tArgs=new TServer.Args(serverSocket);
        tArgs.processor(tProcessor);
        tArgs.protocolFactory(new TBinaryProtocol.Factory());
        TServer server = new TSimpleServer(tArgs);
        server.serve();
    }

    /**
     * TThreadPoolServer
     * 线程池服务模型，使用标准的阻塞式IO，预先创建一组线程处理请求
     * 说明:
     * 有一个专用线程接受连接
     * 一旦一个连接被接受了，被安排给ThreadPoolExecutor中一个工作线程来处理
     * 这个工作线程服务该指定client连接直到关闭。一旦该连接关闭，该工作线程回到线程池
     * 你可以配置线程池的最小和最大线程数。对应的默认值事5和Integer.MAX_VALUE
     * 这意味着如果有10000个并发client连接，你需要运行10000个线程。就本身而论，
     * 这不如其他servers对资源友好。并且，如果client的数量超过线程池的最大数值，请求将被阻塞住直到有工作线程可用。
     * 话虽如此，TThreadPoolServer表现的非常好；我用它支撑10000个并发连接没有任何问题。
     * 如果你能提前知道你的client数目并且也不介意多一点线程，TThreadPoolServer对你可能是个好选择。
     * @throws Exception
     */
    @Test
    public void test02() throws Exception{
        System.out.println("TThreadPoolServer start ...");
        TProcessor tProcessor=new TService.Processor<TService.Iface>(new TServiceImpl());
        TServerSocket serverSocket = new TServerSocket(port02);
        TThreadPoolServer.Args tArgs = new TThreadPoolServer.Args(
                serverSocket);
        tArgs.minWorkerThreads(1);// 设置最小线程数
        tArgs.maxWorkerThreads(1);// 设置最大线程数
        tArgs.processor(tProcessor);
        tArgs.protocolFactory(new TBinaryProtocol.Factory());
        // 线程池服务模型，使用标准的阻塞式IO，预先创建一组线程处理请求。
        TServer server = new TThreadPoolServer(tArgs);
        server.serve();
    }

    /**
     * TNonblockingServer
     * 使用非阻塞式IO，服务端和客户端需要指定 TFramedTransport 数据传输的方式。
     * @throws Exception
     */
    @Test
    public void test03() throws Exception{
        System.out.println("TNonblockingServer start ...");
        TProcessor tProcessor=new TService.Processor<TService.Iface>(new TServiceImpl());
        TNonblockingServerSocket serverSocket = new TNonblockingServerSocket(port03);
        TNonblockingServer.Args tArgs = new TNonblockingServer.Args(serverSocket);
        tArgs.processor(tProcessor);
        tArgs.transportFactory(new TFramedTransport.Factory());
        tArgs.protocolFactory(new TCompactProtocol.Factory());
        TServer server = new TNonblockingServer(tArgs);
        server.serve();
    }

    /**
     * THsHaServer
     * 半同步半异步的服务端模型，需要指定为： TFramedTransport 数据传输的方式。
     * @throws Exception
     */
    @Test
    public void test04() throws Exception{
        System.out.println("THsHaServer start ...");
        TProcessor tProcessor=new TService.Processor<TService.Iface>(new TServiceImpl());
        TNonblockingServerSocket tnbSocketTransport = new TNonblockingServerSocket(port04);
        THsHaServer.Args tArgs = new THsHaServer.Args(tnbSocketTransport);
        tArgs.processor(tProcessor);
        tArgs.transportFactory(new TFramedTransport.Factory());
        tArgs.protocolFactory(new TBinaryProtocol.Factory());
        //半同步半异步的服务模型
        TServer server = new THsHaServer(tArgs);
        server.serve();
    }
}
