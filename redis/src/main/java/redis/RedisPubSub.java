package redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

@SuppressWarnings("all")
@Component
@EnableAutoConfiguration
public class RedisPubSub {

    @Autowired
    RedisTemplate<String, String> redisTemplate;

    @Bean
    RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory factory) {
        System.out.println("init redis sub container with factory: " + factory);
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        return container;
    }

    public void sendMessage(String name, String channel, String message) {
        redisTemplate.convertAndSend(channel, String.format("%s:%s", name, message));
    }

    public static class WudiChatRoomMessageListener implements MessageListener {
        private String name;

        public WudiChatRoomMessageListener(String name) {
            this.name = name;
        }

        @Override
        public void onMessage(Message message, byte[] pattern) {
            String channel = new String(message.getChannel());
            String body = new String(message.getBody());
            int pos = body.indexOf(":");
            String sender = body.substring(0, pos);
            String sentence = body.substring(pos + 1);
            String status = sender.equals(name) ? "send" : "recv";
            System.out.printf("[%s]Chatroom %s - %8s: %s\n", status, channel, sender, sentence);
        }
    }
}
