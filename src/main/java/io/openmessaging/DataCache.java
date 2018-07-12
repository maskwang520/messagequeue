package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by maskwang on 18-7-12.
 */
public class DataCache {
    public ByteBuffer dataBuffer = ByteBuffer.allocate(1024);
    public int count;
}
