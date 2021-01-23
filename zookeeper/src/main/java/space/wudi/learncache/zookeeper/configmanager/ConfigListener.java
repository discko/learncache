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

public class ConfigListener {
    private static final Logger logger = LoggerFactory.getLogger(ConfigListener.class);
    private ZooKeeper zk;
    private String root;

    public ConfigListener(String root) throws IOException, InterruptedException {
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

    private void checkConnect() throws IOException, InterruptedException {
        if(!this.zk.getState().isConnected()){
            connect();
        }
    }

    public void listen(
            String topic,
            Consumer<ZooKeeperResult<Config>> onChange,
            boolean rewatch,
            @Nullable Consumer<String> onNotExist,
            @Nullable Consumer<String> onDelete,
            @Nullable Predicate<String> onExists,
            @Nullable Predicate<String> onCreate
    ) throws IOException, InterruptedException {
        checkConnect();
        ListenWatcherCallback listenWatcherCallback = new ListenWatcherCallback(onNotExist, onExists, onChange, onCreate, onDelete, rewatch);
        zk.exists(topic, listenWatcherCallback, listenWatcherCallback, topic);
    }

    private class ListenWatcherCallback extends ZooKeeperResultHolder<Config> implements AsyncCallback.StatCallback, AsyncCallback.DataCallback,Watcher {

        Consumer<String> onNotExist;
        Predicate<String> onExists;
        Consumer<ZooKeeperResult<Config>> onChange;
        Predicate<String> onCreate;
        Consumer<String> onDelete;
        boolean keepWatchingWhenChange;

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

        // exist callback
        @Override
        public void processResult(int rc, String path, Object topic, Stat stat) {
            Code code = Code.get(rc);
            result.setCode(code);
            switch(code){
                case NONODE:
                    logger.info("topic {} not exists", topic);
                    if(onNotExist != null){
                        onNotExist.accept((String)topic);
                    }
                    break;
                case OK:
                    logger.info("topic {} exists", topic);
                    if(onExists != null && onExists.test((String)topic)){
                        readConfig(path);
                    }
                    break;
                default:
                    logger.warn("unexpected code after check existence of {}: {}", path, code);
            }
        }

        // watcher
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
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
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
