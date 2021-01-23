package space.wudi.learncache.zookeeper.util;

import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperResult<T> {
    private Code code;
    private T data;
    private Throwable throwable;
    private Stat stat;

    ZooKeeperResult() {
    }

    public Code getCode() {
        return code;
    }

    public void setCode(Code code) {
        this.code = code;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public Stat getStat() {
        return stat;
    }

    public void setStat(Stat stat) {
        this.stat = stat;
    }

    @Override
    public String toString() {
        return "ZooKeeperResult{" +
                "code=" + code +
                ", data=" + data +
                ", throwable=" + throwable +
                ", stat=" + stat +
                '}';
    }
}
