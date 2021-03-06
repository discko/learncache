package space.wudi.learncache.zookeeper.util;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Component
public class ZooKeeperSession {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperSession.class);

    /**
     * connection string
     */
    private static String SERVERS;
    /**
     * timeout time in millisecond
     */
    private static Integer TIMEOUT;

    @Value("${zookeeper.servers}")String servers;
    @Value("${zookeeper.timeout}")Integer timeout;
    @PostConstruct
    void postConstruct(){
        SERVERS = servers;
        TIMEOUT = timeout;
    }

    /**
     *
     * @param root root path
     * @return ZooKeeper Session
     * @throws IOException in cases of network failure
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public static ZooKeeper getZooKeeperClient(String root) throws IOException, InterruptedException {
        CountDownLatch cdl = new CountDownLatch(1);
        final ZooKeeper zk = new ZooKeeper(SERVERS + root, TIMEOUT, event -> {
            switch(event.getState()){
                case SyncConnected:
                    logger.info("ZooKeeper connected");
                    cdl.countDown();
                    break;
                default:
                    logger.info("ZooKeeper Connection {}", event.getState());
            }
        });
        cdl.await();
        return zk;
    }

}
