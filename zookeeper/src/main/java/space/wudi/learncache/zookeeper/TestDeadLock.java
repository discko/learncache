package space.wudi.learncache.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import space.wudi.learncache.zookeeper.util.ZooKeeperSession;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("all")
@SpringBootApplication
public class TestDeadLock {
    public static void main(String[] args) throws IOException, InterruptedException {
        SpringApplication.run(TestDeadLock.class);
        ZooKeeper zk = ZooKeeperSession.getZooKeeperClient("/locks");
        CountDownLatch cdlOutside = new CountDownLatch(1);
        System.out.println("before outside zk");
        zk.create("/deadlock",null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, ((rc, path, ctx, name) -> {
            System.out.println("before mid zk");
            CountDownLatch cdlMid = new CountDownLatch(1);
            zk.create("/deadlock2", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, ((rc1, path1, ctx1, name1) -> {
                System.out.println("before inside zk");
                CountDownLatch cdlInside = new CountDownLatch(1);
                zk.create("/deadlock3", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, ((rc2, path2, ctx2, name2) -> {
                    System.out.println("before ininside call countdown");
                    cdlInside.countDown();
                    System.out.println("after ininside call countdown");
                }), null);
                System.out.println("after inside zk");
                try {
                    cdlInside.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("after inside block release");
                cdlMid.countDown();
                System.out.println("after inside call countdown");
            }), null);
            System.out.println("after mid zk");
            try {
                cdlMid.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("after mid block release");
            cdlOutside.countDown();
            System.out.println("after mid call countdown");
        }), null);
        System.out.println("after outside zk");
        cdlOutside.await();
        System.out.println("after outside block release");
        while(true){
            Thread.sleep(1000);
        }
    }
}
