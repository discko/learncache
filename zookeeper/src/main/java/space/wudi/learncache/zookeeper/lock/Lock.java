package space.wudi.learncache.zookeeper.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import space.wudi.learncache.zookeeper.util.ZooKeeperResult;
import space.wudi.learncache.zookeeper.util.ZooKeeperResultHolder;
import space.wudi.learncache.zookeeper.util.ZooKeeperSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Lock {
    private static final Logger logger = LoggerFactory.getLogger(Lock.class);
    private static final String ROOT = "/locks";

    private String lockName;
    private String realLockName;
    private CountDownLatch cdl;
    private ZooKeeper zk;

    public Lock(String lock){
        this.lockName = lock;
        this.cdl = new CountDownLatch(1);
    }

    public void lock() throws Throwable {
        // lazy connect to server
        zk = ZooKeeperSession.getZooKeeperClient(ROOT);
        LockWatcherCallback lockWatcherCallback = new LockWatcherCallback(/*cdl*/);
        ZooKeeperResult<String> result = lockWatcherCallback.getResult();
        // create ephemeral sequential node
        zk.create("/"+lockName+"/"+lockName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, lockWatcherCallback, lockName);
        // block until countdown
        cdl.await();
        if(result.getThrowable() != null){
            throw result.getThrowable();
        }
    }

    public void unlock() {
        if(zk == null || !zk.getState().isConnected()){
            return;
        }
        try {
            zk.delete(realLockName, -1);
        } catch (KeeperException.NoNodeException ignore) {
        } catch (Exception e){
            logger.error("exception while unlock {}: {} ", realLockName, e);
        }
        logger.info("unlock {} finished", realLockName);
    }

    private class LockWatcherCallback extends ZooKeeperResultHolder<String> implements AsyncCallback.StringCallback, Watcher, AsyncCallback.StatCallback {

//        private CountDownLatch cdl;

        LockWatcherCallback(/*CountDownLatch cdl*/) {
//            this.cdl = cdl;
        }

        /**
         * create callback
         */
        @Override
        public void processResult(int rc, String path, Object lockName, String name) {
            Code code = Code.get(rc);
            result.setCode(code);
            switch(code){
                case OK:
                    // node create success
                    // store the real node name, format like "/path/{lockName}{serialNumber}"
                    realLockName = name;
                    try {
                        // check if node the first (gain the lock) or behind (block and watch first)
                        gainLockOrStartWatcher((String)lockName);
                    } catch (InterruptedException | IOException e) {
                        result.setThrowable(e);
                        cdl.countDown();
                    }
                    break;
                case NONODE:
                    // create failed. probably parent node not exists.
                    try {
                        // recursively create parent node
                        waitAndCreateParentNode(path);
                        if(result.getData() == null){
                            // create failed
                            if(result.getThrowable() == null){
                                result.setThrowable(new RuntimeException("failed to create parent node for lock "+lockName));
                            }
                        }else{
                            // create parent success
                            logger.info("creating lock node {}...", lockName);
                            // recreate lock node
                            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, this, lockName);
                        }
                    } catch (IOException | InterruptedException e) {
                        logger.warn("get zk session failed while create parent node: {}:{}", e.getClass(), e.getMessage());
                        result.setData(null);
                        result.setStat(null);
                        result.setThrowable(e);
                    }
                    break;
                default:
                    // otherwise unexpected things happened. release the block
                    logger.warn("unexpected code when create node {}: {}", path, code);
                    result.setData(null);
                    result.setStat(null);
                    result.setThrowable(new RuntimeException("unexpected code when create node " + path+": "+ code));
                    cdl.countDown();
            }
        }

        /**
         * Try to gain lock if the node is at the first
         * or start a watcher to monitor previous lock.
         * <br/>
         * It is an implement of fair lock
         */
        private void gainLockOrStartWatcher(String lockName) throws InterruptedException, IOException {
            CountDownLatch cdlForGetChildren = new CountDownLatch(1);
            List<String> children = new ArrayList<>();
            logger.info("getting children of /", lockName);
            // do not use field zk to getChildren. one connection can only be blocked once!!!
            ZooKeeperSession.getZooKeeperClient(ROOT).getChildren("/"+lockName, false, (int rc, String path, Object ctx, List<String> childNodes)->{
                if(Code.get(rc) == Code.OK){
                    children.addAll(childNodes);
                }
                cdlForGetChildren.countDown();
            }, null);
            // wait until getChildren returned
            cdlForGetChildren.await();
            logger.info("children of /{}: {}",lockName, children);
            // sort children node names.
            Collections.sort(children);

            // List<String> children only contains pure node name,
            // path not included, '/' also not included
            // so findStr should only be a substring from the last '/'
            String findStr = realLockName.replace("/"+lockName+"/", "");
            int order = children.indexOf(findStr);
            logger.info("gain node {} at the order of {}", findStr, order);
            if(order == 0){
                // if this node is the first
                // lock belongs
                result.setData(realLockName);
                result.setStat(null);
                result.setThrowable(null);
                cdl.countDown();
            }else{
                // else, should watch the previous child node
                zk.exists("/"+lockName+"/"+children.get(order-1), this, this, realLockName);
            }
        }

        /**
         * recursively create parent node of path synchronously
         * remember to check the this.result to ensure create successfully
         * NOTE: A better way to perform is to add a new CreateParentResult object into
         *      param list (like "<code>void waitAndCreateParentNode(String path, CreateParentResult result)</code>").
         *      When recurse inside, use the same CreateParentResult object.
         *      In this way, result of waitAndCreateParentNode would not pollute <code>this.result</code>
         */
        private void waitAndCreateParentNode(String path) throws IOException, InterruptedException {
            logger.info("parent of node {} not exists while locking {}", path, lockName);
            // get parent path. in other word, remove the last '/'
            String parentPath = path.substring(0, path.lastIndexOf("/"));
            if(parentPath.length()<1){
                // if parentPath.length < 1, the root of this zk is not exists.
                // this zk cannot create the grandparent any more
                result.setData(null);
                result.setStat(null);
                result.setThrowable(new RuntimeException("parent node create failed. Root Node "+ROOT+" not exists."));
                return;
            }
            // start a new zk to create parent synchronously.
            // NOTE: we cannot use another thread blocker to synchronize same zk in callback,
            //       so we should create another ZooKeeper Client
            CountDownLatch cdlForCreateParent = new CountDownLatch(1);
            ZooKeeperSession.getZooKeeperClient(ROOT).create(
                    parentPath,
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT,
                    (rc1, path1, context, name1) -> {
                        CountDownLatch createParentCdl = (CountDownLatch)context;
                        Code code1 = Code.get(rc1);
                        result.setCode(code1);
                        switch(code1){
                            case OK:
                                // create parent success
                                logger.info("parent node {} created for lock {}", path1, lockName);
                                result.setData(name1);
                                createParentCdl.countDown();    // release the block
                                break;
                            case NONODE:
                                // parent of the parent is not exists, recursively call.
                                try {
                                    waitAndCreateParentNode(path1);
                                } catch (IOException | InterruptedException e) {
                                    e.printStackTrace();
                                    result.setData(null);
                                    result.setStat(null);
                                    result.setThrowable(e);
                                }
                                createParentCdl.countDown();
                                break;
                            case NODEEXISTS:
                                // surprise! non-existent parent node exists now.
                                // probably another thread/client created it just now
                                // pretend I create it
                                logger.info("parent node {} already exists for lock {}", path1, lockName);
                                result.setData(path1);
                                result.setStat(null);
                                result.setThrowable(null);
                                createParentCdl.countDown();    // release the block
                                break;
                            default:
                                logger.warn("parent node {} create failed for lock {} with code {}", path1, lockName, code1);
                                result.setData(null);
                                result.setStat(null);
                                result.setThrowable(new RuntimeException("parent node "+path1+" create failed for lock "+lockName+" with code"+code1));
                                createParentCdl.countDown();
                        }
                    },
                    cdlForCreateParent);
            // block and wait until parent created or cannot create anymore
            cdlForCreateParent.await();
        }

        /**
         * callback method for exists in {@link #gainLockOrStartWatcher}
         */
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            Code code = Code.get(rc);
            result.setCode(code);
            switch(code){
                case NONODE:
                    // previous node become not exist.
                    // probably had been delete just now
                    // directly gain the lock
                    try {
                        gainLockOrStartWatcher(lockName);
                    } catch (InterruptedException | IOException e) {
                        e.printStackTrace();
                        cdl.countDown();
                    }
                    break;
                case OK:
                    // previous node still exists. do nothing.
                    // this operation is to set a watcher
                    logger.info("waiting for {} releasing", path);
                    break;
                default:
                    result.setData(null);
                    result.setStat(null);
                    result.setThrowable(new RuntimeException("unexpected return code when exists "+code));
                    cdl.countDown();
            }
        }

        /**
         * watcher event for previous node. set in {@link #gainLockOrStartWatcher}
         */
        @Override
        public void process(WatchedEvent event) {
            switch (event.getType()){
                case NodeDeleted:
                    // when previous node being deleted
                    try {
                        // gain the lock
                        gainLockOrStartWatcher(lockName);
                    } catch (InterruptedException | IOException e) {
                        e.printStackTrace();
                        cdl.countDown();
                    }
                default:
                    logger.info("state of previous lock {} changes to {}", event.getPath(), event.getType());
            }
        }
    }
}
