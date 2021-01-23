package space.wudi.learncache.zookeeper.util;

public class ZooKeeperResultHolder<T> {
    protected ZooKeeperResult<T> result;

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected ZooKeeperResultHolder() {
        result = new ZooKeeperResult();
    }

    public ZooKeeperResult<T> getResult() {
        return result;
    }
}
