package io.masterkun.nfsonhdfs.util.memory;

import java.nio.ByteBuffer;

/**
 * Represents a pool of direct {@link ByteBuffer} objects.
 *
 * @author Jacob G.
 * @since February 23, 2019
 */
public final class DirectByteBufferPool extends AbstractBufferPool {

    @Override
    protected ByteBuffer allocate(int capacity) {
        return ByteBuffer.allocateDirect(capacity);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if {@code buffer} is not direct.
     */
    @Override
    public void give(ByteBuffer buffer) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("A non-direct ByteBuffer cannot be given to a DirectByteBufferPool!");
        }
        super.give(buffer);
    }
}
