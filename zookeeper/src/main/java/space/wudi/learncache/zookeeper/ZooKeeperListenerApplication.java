package space.wudi.learncache.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import space.wudi.learncache.zookeeper.configmanager.ConfigListener;

import java.io.IOException;

@SpringBootApplication
public class ZooKeeperListenerApplication {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperListenerApplication.class);

    @SuppressWarnings("all")
    public static void main(String[] args) {
        SpringApplication.run(ZooKeeperListenerApplication.class);
        try {
            ConfigListener listener = new ConfigListener(ConfigPath.root);
            boolean loadConfigOnExists = true;
            boolean loadConfigOnCreate = true;
            listener.listen(
                    ConfigPath.topic1,
                    configZooKeeperResult -> {
                        if(KeeperException.Code.OK == configZooKeeperResult.getCode()){
                            logger.info("get new config from {}: {}@{}", ConfigPath.topic1, configZooKeeperResult.getData(), configZooKeeperResult.getStat());
                        }else{
                            logger.warn("find config but cannot apply. code = {}, throwable = {}", configZooKeeperResult.getCode(), configZooKeeperResult.getThrowable());
                        }
                    },
                    false,
                    path -> logger.info("{} not exists", path),
                    path -> logger.info("{} has been deleted", path ),
                    path -> {
                        logger.info("{} has already exists. load config? {}", path, loadConfigOnExists ? "yes" : "no");
                        return loadConfigOnExists;
                    },
                    path -> {
                        logger.info("{} just created. load config? {}", path, loadConfigOnCreate ? "yes" : "no");
                        return loadConfigOnCreate;
                    }
            );
            listener.listen(
                    ConfigPath.topic2,
                    configZooKeeperResult -> {
                        if(KeeperException.Code.OK == configZooKeeperResult.getCode()){
                            logger.info("get new config from {}: {}@{}", ConfigPath.topic2, configZooKeeperResult.getData(), configZooKeeperResult.getStat());
                        }else{
                            logger.warn("find config but cannot apply. code = {}, throwable = {}", configZooKeeperResult.getCode(), configZooKeeperResult.getThrowable());
                        }
                    },
                    true,
                    path -> logger.info("{} not exists", path),
                    path -> logger.info("{} has been deleted", path ),
                    path -> {
                        logger.info("{} has already exists. load config? {}", path, !loadConfigOnExists ? "yes" : "no");
                        return !loadConfigOnExists;
                    },
                    path -> {
                        logger.info("{} just created. load config? {}", path, !loadConfigOnCreate ? "yes" : "no");
                        return !loadConfigOnCreate;
                    }
            );
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        try {
            while(true){
                Thread.sleep(0);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
