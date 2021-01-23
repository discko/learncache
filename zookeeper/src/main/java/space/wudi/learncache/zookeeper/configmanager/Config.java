package space.wudi.learncache.zookeeper.configmanager;

import space.wudi.learncache.zookeeper.util.MySerializable;

/**
 * pretend to be the config that would be published to and listened from zookeeper
 */
public class Config implements MySerializable {
    private static final long serialVersionUID = 1L;

    private String config;

    public Config(String config) {
        this.config = config;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    @Override
    public String toString() {
        return "Config{" +
                "config='" + config + '\'' +
                '}';
    }
}
