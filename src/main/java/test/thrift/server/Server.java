package test.thrift.server;

import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.junit.Test;
import test.thrift.api.TService;
import test.thrift.impl.TServiceImpl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by nihao on 16/11/27.
 */
public class Server {
    private final int simple_port=8001;
    private final int port02=8002;
    private final int port03=8003;
    private final int port04=8004;
    private final int port05=8005;

    /**
     * TSimpleServer
     * 简单的单线程服务模型
     * @throws Exception
     */
    @Test
    public void testSimple() throws Exception {
        TProcessor tProcessor=new TService.Processor<TService.Iface>(new TServiceImpl());
        TServerSocket serverSocket=new TServerSocket(simple_port);
        TServer.Args tArgs=new TServer.Args(serverSocket);
        tArgs.processor(tProcessor);
        tArgs.protocolFactory(new TBinaryProtocol.Factory());
        TServer server = new TSimpleServer(tArgs);
        System.out.println("simple server start ...");
        server.serve();
    }

    /**
     * TThreadPoolServer
     * 线程池服务模型
     * @throws Exception
     */
    @Test
    public void test02() throws Exception{
        TProcessor tProcessor=new TService.Processor<TService.Iface>(new TServiceImpl());
        TServerSocket serverSocket = new TServerSocket(port02);
        TThreadPoolServer.Args tArgs = new TThreadPoolServer.Args(
                serverSocket);
        tArgs.minWorkerThreads(2);// 设置最小线程数
        tArgs.maxWorkerThreads(80);// 设置最大线程数
        tArgs.processor(tProcessor);
        tArgs.protocolFactory(new TBinaryProtocol.Factory());
        // 线程池服务模型，使用标准的阻塞式IO，预先创建一组线程处理请求。
        TServer server = new TThreadPoolServer(tArgs);
        System.out.println("TThreadPoolServer start ...");
        server.serve();
    }

    /**
     * TNonblockingServer
     * 使用非阻塞式IO，服务端和客户端需要指定 TFramedTransport 数据传输的方式。
     * @throws Exception
     */
    @Test
    public void test03() throws Exception{
        TProcessor tProcessor=new TService.Processor<TService.Iface>(new TServiceImpl());
        TNonblockingServerSocket serverSocket = new TNonblockingServerSocket(port03);
        TNonblockingServer.Args tArgs = new TNonblockingServer.Args(serverSocket);
        tArgs.processor(tProcessor);
        tArgs.transportFactory(new TFramedTransport.Factory());
        tArgs.protocolFactory(new TCompactProtocol.Factory());
        TServer server = new TNonblockingServer(tArgs);
        System.out.println("TNonblockingServer start ...");
        server.serve();
    }

    /**
     * THsHaServer
     * 半同步半异步的服务端模型，需要指定为： TFramedTransport 数据传输的方式。
     * @throws Exception
     */
    @Test
    public void test04() throws Exception{
        TProcessor tProcessor=new TService.Processor<TService.Iface>(new TServiceImpl());
        TNonblockingServerSocket tnbSocketTransport = new TNonblockingServerSocket(port04);
        THsHaServer.Args tArgs = new THsHaServer.Args(tnbSocketTransport);
        tArgs.processor(tProcessor);
        tArgs.transportFactory(new TFramedTransport.Factory());
        tArgs.protocolFactory(new TBinaryProtocol.Factory());
        //半同步半异步的服务模型
        TServer server = new THsHaServer(tArgs);
        System.out.println("THsHaServer start ...");
        server.serve();
    }

    /**
     * TThreadedSelectorServer
     * @throws Exception
     */
    @Test
    public void test05() throws Exception{
        // 关联处理器与Service服务的实现
        TProcessor tProcessor = new TService.Processor<TService.Iface>(new TServiceImpl());
        // 传输通道 - 非阻塞方式
        TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port05);
        // 目前Thrift提供的最高级的模式，可并发处理客户端请求
        TThreadedSelectorServer.Args tArgs = new TThreadedSelectorServer.Args(serverTransport);
        tArgs.processor(tProcessor);
        // 设置传输工厂，使用非阻塞方式，按块的大小进行传输，类似于Java中的NIO
        tArgs.transportFactory(new TFramedTransport.Factory());
        // 设置协议工厂，高效率的、密集的二进制编码格式进行数据传输协议
        tArgs.protocolFactory(new TCompactProtocol.Factory());
        // 设置处理器工厂,只返回一个单例实例
        tArgs.processorFactory(new TProcessorFactory(tProcessor));
        // 多个线程，主要负责客户端的IO处理
        tArgs.selectorThreads(10);
        // 工作线程池
        ExecutorService pool = Executors.newFixedThreadPool(500);
        tArgs.executorService(pool);
        TServer server = new TThreadedSelectorServer(tArgs);
        System.out.println("TThreadedSelectorServer start ...");
        server.serve();
    }
}
