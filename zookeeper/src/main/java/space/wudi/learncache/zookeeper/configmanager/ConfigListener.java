package space.wudi.learncache.zookeeper.configmanager;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import space.wudi.learncache.zookeeper.util.MySerializable;
import space.wudi.learncache.zookeeper.util.ZooKeeperResult;
import space.wudi.learncache.zookeeper.util.ZooKeeperResultHolder;
import space.wudi.learncache.zookeeper.util.ZooKeeperSession;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * a listener to receive config changes.
 * one listener can listen to multiple config nodes.
 */
public class ConfigListener {
    private static final Logger logger = LoggerFactory.getLogger(ConfigListener.class);
    /**
     * zookeeper session (or connection)
     */
    private ZooKeeper zk;
    /**
     * remembers the root to listening
     */
    private String root;

    /**
     * create a listener to listen to root
     * @param root the root to be listened
     * @throws IOException in cases of network failure
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public ConfigListener(String root) throws IOException, InterruptedException {
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
    private void checkConnect() throws IOException, InterruptedException {
        if(!this.zk.getState().isConnected()){
            connect();
        }
    }

    /**
     * start to listen to a config node at path
     * @param path a path relative to {@link #root} passed when construct
     * @param onChange do something when config path node changes
     * @param keepWatchingWhenChange after reading config, whether watch this node's change or not
     * @param onNotExist do something if config path node not exists at the beginning
     * @param onDelete whether read the config or not if node of config path exists at the beginning
     * @param onExists whether read the config or not if node of config path exists at the beginning
     * @param onCreate whether read the config or not if the node of config path create
     * @throws IOException in cases of network failure
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void listen(
            String path,
            Consumer<ZooKeeperResult<Config>> onChange,
            boolean keepWatchingWhenChange,
            @Nullable Consumer<String> onNotExist,
            @Nullable Consumer<String> onDelete,
            @Nullable Predicate<String> onExists,
            @Nullable Predicate<String> onCreate
    ) throws IOException, InterruptedException {
        checkConnect();
        ListenWatcherCallback listenWatcherCallback = new ListenWatcherCallback(onNotExist, onExists, onChange, onCreate, onDelete, keepWatchingWhenChange);
        zk.exists(path, listenWatcherCallback, listenWatcherCallback, path);
    }

    /**
     * an object combine watcher and callback for ConfigListener
     */
    private class ListenWatcherCallback extends ZooKeeperResultHolder<Config> implements AsyncCallback.StatCallback, AsyncCallback.DataCallback, Watcher {

        /**
         * do something if config path node not exists at the beginning
         */
        private Consumer<String> onNotExist;
        /**
         * if node of config path exists at the beginning, whether read the config or not
         */
        private Predicate<String> onExists;
        /**
         * do something when config path node changes
         */
        private Consumer<ZooKeeperResult<Config>> onChange;
        /**
         * if the node of config path create, whether read the config or not
         */
        private Predicate<String> onCreate;
        /**
         * do something if config path node is removed
         */
        private Consumer<String> onDelete;
        /**
         * after reading config, whether watch this node's change or not
         */
        private boolean keepWatchingWhenChange;

        /**
         * create Callback and Watcher fro Config Listener
         * @param onNotExist do something if configPath node not exists at the beginning
         * @param onExists if node of configPath exists at the beginning, whether read the config or not
         * @param onChange do something when configPath node changes
         * @param onCreate if the node of configPath create, whether read the config or not
         * @param onDelete do something if configPath node is removed
         * @param keepWatchingWhenChange after reading config, whether watch this node's change or not
         */
        ListenWatcherCallback(
                Consumer<String> onNotExist,
                Predicate<String> onExists,
                Consumer<ZooKeeperResult<Config>> onChange,
                Predicate<String> onCreate,
                Consumer<String> onDelete,
                boolean keepWatchingWhenChange)
        {
            this.onNotExist = onNotExist;
            this.onExists = onExists;
            this.onChange = onChange;
            this.onCreate = onCreate;
            this.onDelete = onDelete;
            this.keepWatchingWhenChange = keepWatchingWhenChange;
        }

        /**
         * a callback method when zookeeper.exists
         * @param rc return code. use KeeperException.Code to convert to the enumerable
         * @param path query path
         * @param configPath param ctx of zookeeper.exists
         * @param stat node stat info. only exists when node exists
         */
        @Override
        public void processResult(int rc, String path, Object configPath, Stat stat) {
            Code code = Code.get(rc);
            result.setCode(code);
            switch(code){
                case NONODE:
                    logger.info("configPath {} not exists", configPath);
                    if(onNotExist != null){
                        onNotExist.accept((String)configPath);
                    }
                    break;
                case OK:
                    logger.info("configPath {} exists", configPath);
                    if(onExists != null && onExists.test((String)configPath)){
                        readConfig(path);
                    }
                    break;
                default:
                    logger.warn("unexpected code after check existence of {}: {}", path, code);
            }
        }

        /**
         * watcher event
         * @param event event object
         */
        @Override
        public void process(WatchedEvent event) {
            switch (event.getType()){
                case NodeCreated:
                    logger.info("node {} created", event.getPath());
                    if(onCreate != null && onCreate.test(event.getPath())){
                        readConfig(event.getPath());
                    }else{
                        zk.exists(event.getPath(), this, this, event.getPath());
                    }
                    break;
                case NodeDeleted:
                    logger.info("node {} deleted", event.getPath());
                    if(onDelete != null){
                        onDelete.accept(event.getPath());
                    }
                    zk.exists(event.getPath(), this, this, event.getPath());
                    break;
                case NodeDataChanged:
                    logger.info("node {} data changed", event.getPath());
                    readConfig(event.getPath());
                    break;
                default:
                    logger.warn("unsupported watched event: {}", event);
            }
        }

        // get data callback

        /**
         * callback method of zookeeper.getData
         * @param rc return code. use KeeperException.Code to convert to the enumerable
         * @param path query path
         * @param configPath param ctx of zookeeper.exists
         * @param data data in node
         * @param stat node stat info. only exists when node exists
         */
        @Override
        public void processResult(int rc, String path, Object configPath, byte[] data, Stat stat) {
            Code code = Code.get(rc);
            result.setCode(code);
            switch(code){
                case OK:
                    logger.info("get return data from {}. size = {}", path, data.length);
                    try{
                        result.setData(MySerializable.fromBytes(data, Config.class));
                        result.setStat(stat);
                        result.setThrowable(null);
                    }catch(Exception e){
                        result.setThrowable(e);
                    }
                    break;
                default:
                    logger.warn("unexpected code after get data on {} : {}", path, code);
            }
            onChange.accept(result);
        }

        /**
         * to read the config at path
         * @param path query path
         */
        private void readConfig(String path){
            if(keepWatchingWhenChange){
                logger.info("get data at {} and reset watcher", path);
                zk.getData(path, this, this, path);
            }else{
                logger.info("get data at {} and remove watcher", path);
                zk.getData(path, false, this, path);
            }
        }
    }
}
