package io.masterkun.nfsonhdfs.util.memory;

import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.memory.ByteBufferAware;
import org.glassfish.grizzly.memory.DefaultMemoryManagerFactory;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.memory.PooledMemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CustomPooledMemoryManagerFactory implements DefaultMemoryManagerFactory {

    public static final DirectByteBufferPool BYTE_BUFFER_POOL = new DirectByteBufferPool();
    private static final Logger LOG =
            LoggerFactory.getLogger(CustomPooledMemoryManagerFactory.class);

    public CustomPooledMemoryManagerFactory() {
        LOG.info("Init CustomPooledMemoryManagerFactory");
    }

    @Override
    public MemoryManager<Buffer> createMemoryManager() {
        return new CustomizedMemoryManager(
                PooledMemoryManager.DEFAULT_BASE_BUFFER_SIZE,
                PooledMemoryManager.DEFAULT_NUMBER_OF_POOLS,
                PooledMemoryManager.DEFAULT_GROWTH_FACTOR,
                Runtime.getRuntime().availableProcessors(),
                PooledMemoryManager.DEFAULT_HEAP_USAGE_PERCENTAGE,
                PooledMemoryManager.DEFAULT_PREALLOCATED_BUFFERS_PERCENTAGE,
                true
        );
    }

    public static class CustomizedMemoryManager extends PooledMemoryManager implements ByteBufferAware {

        private final boolean direct;

        public CustomizedMemoryManager() {
            direct = true;
        }

        public CustomizedMemoryManager(boolean isDirect) {
            super(isDirect);
            direct = isDirect;
        }

        public CustomizedMemoryManager(int baseBufferSize, int numberOfPools, int growthFactor,
                                       int numberOfPoolSlices, float percentOfHeap,
                                       float percentPreallocated, boolean isDirect) {
            super(baseBufferSize, numberOfPools, growthFactor, numberOfPoolSlices, percentOfHeap,
                    percentPreallocated, isDirect);
            direct = isDirect;
        }

        @Override
        public ByteBuffer allocateByteBuffer(int size) {
            return BYTE_BUFFER_POOL.take(size).limit(size);
        }

        @Override
        public ByteBuffer allocateByteBufferAtLeast(int size) {
            return BYTE_BUFFER_POOL.take(size);
        }

        @Override
        public ByteBuffer reallocateByteBuffer(ByteBuffer oldByteBuffer, int newSize) {
            return null;
        }

        @Override
        public void releaseByteBuffer(ByteBuffer byteBuffer) {
            BYTE_BUFFER_POOL.give(byteBuffer);
        }
    }
}
