package space.wudi.learncache.redis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Scanner;

@SpringBootApplication
public class RedisApplication{

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(RedisApplication.class, args);
//        testRedisTemplate(context);
//        testRedisPubSub(context);
        testErrorType(context);
    }

    private static void testErrorType(ApplicationContext context){
        System.out.println(Arrays.toString("吴迪".getBytes())+ Charset.defaultCharset());
    }

    private static void testRedisTemplate(ApplicationContext context){
        RedisSetGet redis = context.getBean(RedisSetGet.class);
        String key = "todaykey3";
        String value = "valueof"+key;
        Duration expire = Duration.ofMinutes(5);
        System.out.println(redis.get(key));
        redis.set(key, value, expire);
        String redisReturn = (String)redis.get(key);
        System.out.println(redisReturn);
    }

    private static void testRedisPubSub(ApplicationContext context){
        RedisMessageListenerContainer container = context.getBean(RedisMessageListenerContainer.class);
        String myName = "WuDi";
        String channel = "WudiChatRoom";
        container.addMessageListener(new RedisPubSub.WudiChatRoomMessageListener(myName),
                new ChannelTopic(channel));
        System.out.println("START CHATTING ~");
        RedisPubSub redisPubSub = context.getBean(RedisPubSub.class);
        String message = null;
        Scanner scanner = new Scanner(System.in);
        while(!"quit".equals(message)){
            message = scanner.nextLine();
            redisPubSub.sendMessage(myName, channel, message);
        }
    }

}
