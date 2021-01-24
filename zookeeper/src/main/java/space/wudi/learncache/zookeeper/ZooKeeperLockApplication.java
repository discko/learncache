package space.wudi.learncache.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import space.wudi.learncache.zookeeper.lock.Lock;

@SuppressWarnings("all")
@SpringBootApplication
public class ZooKeeperLockApplication {
    private static Logger logger = LoggerFactory.getLogger(ZooKeeperLockApplication.class);
    public static void main(String[] args) {
        SpringApplication.run(ZooKeeperLockApplication.class);
        final int ThreadCount = 5;
        Thread[] threads = new Thread[ThreadCount];
        for (int i = 0; i < ThreadCount; i++) {
            threads[i] = new Thread(()->{
                Lock lock = new Lock("mylock1");
                try {
                    lock.lock();
                    logger.warn("+++++++Thread {} gain lock", Thread.currentThread().getName());
                    Thread.sleep(2000);
                    logger.info("+++++++Thread {} start to release lock", Thread.currentThread().getName());
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }finally {
                    lock.unlock();
                }
            });
        }

        for (Thread thread: threads) {
            thread.start();
        }

        try{
            while(true){
                Thread.sleep(5000);
                System.out.println("waiting");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
