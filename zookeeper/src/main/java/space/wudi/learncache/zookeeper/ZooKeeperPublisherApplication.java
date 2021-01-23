package space.wudi.learncache.zookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import space.wudi.learncache.zookeeper.configmanager.Config;
import space.wudi.learncache.zookeeper.configmanager.ConfigPublisher;
import space.wudi.learncache.zookeeper.util.ZooKeeperResult;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@SpringBootApplication
public class ZooKeeperPublisherApplication {

    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperPublisherApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ZooKeeperPublisherApplication.class);
        try {
            ConfigPublisher configPublisher = new ConfigPublisher(ConfigPath.root);
            Config config1 = new Config(String.format("config time %s", DateTimeFormatter.ofPattern("Y-MM-DD HH:mm:ss").format(LocalDateTime.now())));
            ZooKeeperResult<String> result1 = configPublisher.publish(config1, ConfigPath.configNode1, null);
            logger.info("first config {} result = {}", ConfigPath.configNode1, result1);

            Thread.sleep(1000);

            Config config2 = new Config(String.format("config time %s", DateTimeFormatter.ofPattern("Y-MM-DD HH:mm:ss").format(LocalDateTime.now())));
            ZooKeeperResult<String> result2 = configPublisher.publish(config2, ConfigPath.configNode2, null);
            logger.info("second config {} result = {}", ConfigPath.configNode2, result2);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
