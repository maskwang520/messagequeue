package io.openmessaging;

/**
 * Created by maskwang on 18-7-9.
 * 块的设计
 */
public class Block {
    //开始位置
    public long startPosition;

    //长度
    public int length;

    public int size;

    public Block(Long startPosition, int length) {
        this.startPosition = startPosition;
        this.length = length;
        this.size = 0;
    }
}
