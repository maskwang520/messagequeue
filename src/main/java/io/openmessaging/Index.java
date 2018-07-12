package io.openmessaging;

public class Index {
    public Long startPosition;
    public Integer length;

    public Index(Long startPosition, Integer length) {
        this.startPosition = startPosition;
        this.length = length;
    }
}
