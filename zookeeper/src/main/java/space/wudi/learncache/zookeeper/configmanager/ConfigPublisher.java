package space.wudi.learncache.zookeeper.configmanager;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.wudi.learncache.zookeeper.util.ZooKeeperResult;
import space.wudi.learncache.zookeeper.util.ZooKeeperResultHolder;
import space.wudi.learncache.zookeeper.util.ZooKeeperSession;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ConfigPublisher {
    private static final Logger logger = LoggerFactory.getLogger(ConfigPublisher.class);

    private ZooKeeper zk;
    private final String root;

    public ConfigPublisher(String root) throws IOException, InterruptedException {
        this.root = root;
        connect();
    }

    private void connect() throws InterruptedException, IOException {
        logger.info("connect to zookeeper server");
        if(this.zk != null){
            zk.close();
        }
        this.zk = ZooKeeperSession.getZooKeeperClient(this.root);
    }

    private void checkConnecting() throws IOException, InterruptedException {
        if(!this.zk.getState().isConnected()){
            connect();
        }
    }

    public ZooKeeperResult<String> publish(Config config, String topic, List<ACL> acls) throws IOException, InterruptedException {
        checkConnecting();
        byte[] configData = config.toBytes();
        List<ACL> realAcls = acls == null ? ZooDefs.Ids.OPEN_ACL_UNSAFE : acls;
        CountDownLatch cdl = new CountDownLatch(1);
        PublishCallback publishCallback = new PublishCallback(cdl);
        zk.create(topic, configData, realAcls, CreateMode.PERSISTENT, publishCallback, configData);
        ZooKeeperResult<String> result = publishCallback.getResult();
        try {
            cdl.await();
        } catch (InterruptedException ignored) {
        }
        return result;
    }

    private class PublishCallback extends ZooKeeperResultHolder<String> implements AsyncCallback.StringCallback, AsyncCallback.StatCallback {

        private final CountDownLatch cdl;

        PublishCallback(CountDownLatch cdl) {
            this.cdl = cdl;
        }
        // create callback
        @Override
        public void processResult(int rc, String path, Object configData, String name) {
            Code code = Code.get(rc);
            result.setCode(code);
            switch(code){
                case NODEEXISTS:
                    logger.info("node exists. updating");
                    zk.setData(path, (byte[])configData, -1, this, configData);
                    break;
                case OK:
                    logger.info("node created.");
                    result.setData(path);
                    cdl.countDown();
                    break;
                default:
                    logger.warn("unexpect rc while creating. rc = {}", code);
                    cdl.countDown();
            }
        }

        // set data callback
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            Code code = Code.get(rc);
            result.setCode(code);
            switch(code){
                case OK:
                    logger.info("update success");
                    result.setData(path);
                    result.setStat(stat);
                    cdl.countDown();
                    break;
                default:
                    logger.warn("unexpect rc while updating. rc = {}", code);
                    cdl.countDown();
            }
        }
    }
}
