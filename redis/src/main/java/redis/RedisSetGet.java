package redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.*;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("all")
@Component
public class RedisSetGet {
    @Autowired
    RedisTemplate redisTemplate;

    @Autowired
    RedisTemplate<Object, Object> objectTemplate;

    @Autowired
    RedisTemplate<String, String> stringTemplate;

    public void set(String key, String value, Duration withExpire) {
        System.out.println(key);
        stringTemplate.opsForValue().set("key", "value");
        stringTemplate.opsForValue().set(key, value, withExpire);
    }

    public Object get(String key) {
        System.out.println(redisTemplate.hashCode());
        return stringTemplate.opsForValue().get(key);
    }

    public List<String> get(List<String> a) {
        return a;
    }

    public void setTwoKey() {
        redisTemplate.opsForValue().set("key1", "value1");
        stringTemplate.opsForValue().set("key2", "value2");
        System.out.println("rT.get(key1)=" + redisTemplate.opsForValue().get("key1"));
        System.out.println("sT.get(key1)=" + stringTemplate.opsForValue().get("key1"));
        System.out.println("rT.get(key2)=" + redisTemplate.opsForValue().get("key2"));
        System.out.println("sT.get(key2)=" + stringTemplate.opsForValue().get("key2"));
    }

    public void setAndGetObject() {
        int[] aaa = new int[]{1, 2, 3, 4, 5};

        redisTemplate.opsForValue().set("keyOfObject", aaa);
        Object getResult = redisTemplate.opsForValue().get("keyOfObject");
        int[] rtArray = (int[]) getResult;
        System.out.println(Arrays.equals(aaa, rtArray));

    }

    public void doTransaction() {
//        ValueOperations<String, String> valueOps = stringTemplate.opsForValue();
//        stringTemplate.setEnableTransactionSupport(true);
//        valueOps.set("keybeforetrans", "bbb");
//        stringTemplate.multi();
//        valueOps.set("keytrans", "aaa");
//        valueOps.set("keytrans2", "bbb");
//        valueOps.get("keytrans");
//        stringTemplate.discard();
        // https://docs.spring.io/spring-data/redis/docs/2.4.3/reference/html/#tx
        stringTemplate.execute(new SessionCallback<Void>() {
            @Override
            public Void execute(RedisOperations operations) throws DataAccessException {
                ValueOperations valueOperations = operations.opsForValue();
                operations.multi();
                valueOperations.set("key1", "value1");
                valueOperations.set("key2", "value2");
                String getResult = (String) valueOperations.get("key1");
                System.out.println("getresult in multi" + getResult);
                operations.exec();
                return null;
            }
        });
    }
}
