package space.wudi.learncache.redis;
        import org.springframework.beans.factory.annotation.Autowired;
        import org.springframework.data.redis.core.RedisTemplate;
        import org.springframework.stereotype.Component;
        import java.time.Duration;
@SuppressWarnings("all")
@Component
public class RedisSetGet {
    @Autowired
    RedisTemplate<String, String> redisTemplate;

    @Autowired
    RedisTemplate<String, String> stringTemplate;

    public void set(String key, String value, Duration withExpire){
        System.out.println(key);
        redisTemplate.opsForValue().set(key, value, withExpire);
    }

    public void increase(String key){
        Long rtValue = redisTemplate.opsForValue().increment(key);
        System.out.println(rtValue);
    }

    public Object get(String key){
        System.out.println(redisTemplate.hashCode());
        return stringTemplate.opsForValue().get(key);
    }

}
