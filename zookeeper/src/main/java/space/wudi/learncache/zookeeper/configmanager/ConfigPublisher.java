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
/**
 * a publisher to send new config or config updates.
 * one publisher can send multiple configs.
 */
public class ConfigPublisher {
    private static final Logger logger = LoggerFactory.getLogger(ConfigPublisher.class);
    /**
     * zookeeper session (or connection)
     */
    private ZooKeeper zk;
    /**
     * remembers the root to listening
     */
    private final String root;
    /**
     * create a publisher to publish config beneath root
     * @param root the root to be listened
     * @throws IOException in cases of network failure
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public ConfigPublisher(String root) throws IOException, InterruptedException {
        this.root = root;
        connect();
    }
    /**
     * connect to zookeeper servers. if already have a connection, release it.
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws IOException in cases of network failure
     */
    private void connect() throws InterruptedException, IOException {
        logger.info("connect to zookeeper server");
        if(this.zk != null){
            zk.close();
        }
        this.zk = ZooKeeperSession.getZooKeeperClient(this.root);
    }
    /**
     * check if current connection is alive. if not, reconnect
     * @throws IOException in cases of network failure
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    private void checkConnecting() throws IOException, InterruptedException {
        if(!this.zk.getState().isConnected()){
            connect();
        }
    }

    /**
     * start to listen to a config node at path
     * @param config the config to be published
     * @param configPath the config to be store at
     * @param acls the Access Control List, null to use default
     * @throws IOException in cases of network failure
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public ZooKeeperResult<String> publish(Config config, String configPath, List<ACL> acls) throws IOException, InterruptedException {
        checkConnecting();
        byte[] configData = config.toBytes();
        List<ACL> realAcls = acls == null ? ZooDefs.Ids.OPEN_ACL_UNSAFE : acls;
        CountDownLatch cdl = new CountDownLatch(1);
        PublishCallback publishCallback = new PublishCallback(cdl);
        zk.create(configPath, configData, realAcls, CreateMode.PERSISTENT, publishCallback, configData);
        ZooKeeperResult<String> result = publishCallback.getResult();
        try {
            cdl.await();
        } catch (InterruptedException ignored) {
        }
        return result;
    }

    /**
     * an object used as multiple callback for ConfigPublisher
     */
    private class PublishCallback extends ZooKeeperResultHolder<String> implements AsyncCallback.StringCallback, AsyncCallback.StatCallback {

        /**
         * synchronizer
         */
        private final CountDownLatch cdl;

        /**
         * create a publisher. use CountDownLatch to wait until results prepared
         * @param cdl the synchronizer
         */
        PublishCallback(CountDownLatch cdl) {
            this.cdl = cdl;
        }

        /**
         * a callback method for zookeeper.create
         * @param rc return code. use KeeperException.Code to convert to the enumerable
         * @param path query path
         * @param configData byte[] of the config
         * @param name node name. usually the same to path unless use {@link CreateMode#PERSISTENT_SEQUENTIAL} or {@link CreateMode#EPHEMERAL_SEQUENTIAL}
         */
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

        /**
         * callback methdo for zookeeper.setData
         * @param rc return code. use KeeperException.Code to convert to the enumerable
         * @param path query path
         * @param stat node stat info. only exists when node exists
         */
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
