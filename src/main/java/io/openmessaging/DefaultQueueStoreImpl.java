package io.openmessaging;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {


    public static Collection<byte[]> EMPTY = new ArrayList<byte[]>();

    //以块为索引
    public Map<String, List<Block>> blockMap = new ConcurrentHashMap<>();

    public Map<String, DataCache> cacheMap = new ConcurrentHashMap<>();
    //父目录路径
    public static final String DIRPATH = "/home/maskwang/Myfolder/message/data/";

    Lock lock = new ReentrantLock();

    //预先创建根目录
    static {
        File file = new File(DIRPATH);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public void put(String queueName, byte[] message) {
        int hash = hashFile(queueName);
        String path = DIRPATH + hash + ".txt";
        lock.lock();
        //创建文件
        File file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (!blockMap.containsKey(queueName)) {
            List<Block> list = new ArrayList();
            blockMap.put(queueName, list);
        }

        if (!cacheMap.containsKey(queueName)) {
            DataCache dataCache = new DataCache();
            cacheMap.put(queueName, dataCache);
        }

        DataCache dataCache = cacheMap.get(queueName);
        if (dataCache.count == 10) {
            FileChannel fileChannel = null;
            // long fileLength = 0;
            try {
                fileChannel = new RandomAccessFile(file, "rw").getChannel();
                //fileLength = raf.length();
            } catch (Exception e) {
                e.printStackTrace();
            }

            long blockPosition;
            try {
                blockPosition = getLeastBlockPosition(getLeastBlockPosition(fileChannel.size()));
                Block block = new Block(blockPosition, dataCache.dataBuffer.position());
                block.size = 10;
                blockMap.get(queueName).add(block);
                dataCache.dataBuffer.flip();
                fileChannel.position(blockPosition);
                fileChannel.write(dataCache.dataBuffer);
                dataCache.dataBuffer.clear();

            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                try {
                    fileChannel.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        } else {
            dataCache.dataBuffer.putInt(message.length);
            dataCache.dataBuffer.put(message);
            dataCache.count++;
        }


//        MappedByteBuffer buffer = null;
//        long blockPosition = 0;
//        try {
//            blockPosition = getLeastBlockPosition(getLeastBlockPosition(fileChannel.size()));
//            // System.out.println(blockPosition);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        //如果还没有创建Block
//        if (blockMap.get(queueName).size() == 0) {
//            Block block = new Block(blockPosition, message.length + 4);
//            block.size++;
//            blockMap.get(queueName).add(block);
//            try {
//                buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, blockPosition, 4 + message.length);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        } else {
//            //如果已经存在block,则放在blcok里面,如果满了,就另外放
//            Block lastBlock = blockMap.get(queueName).get(blockMap.get(queueName).size() - 1);
//
//
//            if (lastBlock.length + message.length + 4 <= 2048) {
//
//                try {
//                    buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, lastBlock.startPosition+lastBlock.length, message.length + 4);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                lastBlock.length = lastBlock.length + message.length + 4;
//                lastBlock.size++;
//            } else {
//                Block block = new Block(blockPosition, message.length + 4);
//                blockMap.get(queueName).add(block);
//                try {
//                    buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, blockPosition, message.length + 4);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                block.size++;
//            }
//        }
//        buffer.putInt(message.length);
//        buffer.put(message);
//        try {
//            fileChannel.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        lock.unlock();


    }


    public Collection<byte[]> get(String queueName, long offset, long num) {

        //队列不存在
        if (!blockMap.containsKey(queueName)) {
            return EMPTY;
        }
        //消息集合
        List<byte[]> msgs = new ArrayList();
        List<Block> blocks = blockMap.get(queueName);

        int hash = hashFile(queueName);
        String path = DIRPATH + hash + ".txt";
        FileChannel fileChannel = null;
        int size = blocks.get(0).size;
        int eleNum = 0;
        //记录了目标block所在的下标
        int blockNum = 0;
        lock.lock();
        try {
            fileChannel = new RandomAccessFile(new File(path), "rw").getChannel();
            for (int i = 1; i < blocks.size() && size < offset; i++, blockNum++) {
                size += blocks.get(i).size;
            }

            size = size - blocks.get(blockNum).size;


            for (int i = blockNum; i < blocks.size(); i++) {
                //size+=blocks.get(i).size;
                // size-=blocks.get(i).size;
                int length = blocks.get(i).length;
                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, blocks.get(i).startPosition, length);
                int sum = 0;
                while (sum < length && size < offset) {
                    int len = buffer.getInt();
                    sum += 4;
                    sum += len;
                    buffer.position(sum);
                    size++;
                }

                if (size >= offset) {
                    while (buffer.position() < length && eleNum <= num) {
                        int len = buffer.getInt();
                        byte[] temp = new byte[len];
                        buffer.get(temp, 0, len);
                        eleNum++;
                        msgs.add(temp);
                    }
                    if (eleNum > num) {
                        break;
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                fileChannel.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            lock.unlock();

        }

        return msgs;
    }


    //根据队列的名字hash到对应的文件中,共16个文件
    int hashFile(String queueName) {
        return queueName.hashCode() & 0xff;
        //return 0;
    }

    //block的大小为2048
    public long getLeastBlockPosition(long length) {
        if (length == 0) {
            return 0;
        }
        int initSize = 1 << 10;
        int i = 1;
        while (i * initSize <= length) {
            i++;
        }
        //定义到可用的块的第一个位置
        return i * initSize;
    }

//    public static void main(String[] args) {
//        System.out.println(getLeastBlockPosition(2048));
//    }


}