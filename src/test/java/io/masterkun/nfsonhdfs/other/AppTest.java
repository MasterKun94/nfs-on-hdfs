package io.masterkun.nfsonhdfs.other;

import java.net.InetAddress;

public class AppTest {

    public static void main(String[] args) throws Exception {
//        CustomPooledMemoryManagerFactory.CustomizedMemoryManager manager = new
//        CustomPooledMemoryManagerFactory.CustomizedMemoryManager(true);
//        final Buffer allocate = manager.allocate(1048376);
//        final Buffer allocate2 = manager.allocate(4096);
//        final ByteBuffer byteBuffer = allocate.toByteBuffer();
//        final ByteBuffer byteBuffer2 = allocate2.toByteBuffer();
//        System.out.println(allocate2);
//        ByteBufferPool largeByteBufferPool = new ElasticByteBufferPool();
//        final ByteBuffer buffer = largeByteBufferPool.getBuffer(true, 1048576);
//        largeByteBufferPool.putBuffer(buffer);
//        final ByteBuffer buffer2 = largeByteBufferPool.getBuffer(true, 1048576);
//        System.out.println(buffer == buffer2);

        byte[] b1 = new byte[1024];
        byte[] b2 = new byte[1024];

        InetAddress address = InetAddress.getByName("10.50.30.126");
        System.out.println(address);

    }
}
