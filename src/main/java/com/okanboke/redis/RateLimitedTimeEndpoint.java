package com.okanboke.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.LocalTime;

@Controller("/time")
public class RateLimitedTimeEndpoint {
    // bir dakikada 10 istekten fazla gelince kuyruğu durdurur bir dakika dolduktan hemen sonra kuyruğu 10 adet kotalı tekrar başlatır
    private static final Logger LOG = LoggerFactory.getLogger(RateLimitedTimeEndpoint.class);
    private static final int QUOTA_PER_MINUTE = 10;
    private StatefulRedisConnection<String, String> redis;

    public RateLimitedTimeEndpoint(final StatefulRedisConnection<String, String> redis) {
        this.redis = redis;
    }


    @Get("/")
    public String time() {
        return getTime("EXAMPLE::UTC", LocalTime.now());
    }

    // istek hızı
    @Get("/utc")
    public String utc() {
        return getTime("EXAMPLE::UTC",LocalTime.now(Clock.systemUTC()));
    }

    private String getTime(final String key, final LocalTime now) {
        final String value = redis.sync().get(key);
        int currentQuota = null == value ? 0 : Integer.parseInt(value);
        if (currentQuota >= QUOTA_PER_MINUTE) {
            final String err = String.format("Rate limit reached %s %s/%s", key, currentQuota, QUOTA_PER_MINUTE);
            LOG.info(err);
            return err;
        }
        LOG.info("Current quota {} in {}/{}", key, currentQuota, QUOTA_PER_MINUTE); // log çıktısı
        increaseCurrentQuota(key);// kotayı arttırma
        return now.toString();
    }

    // maksimum istek
    private void increaseCurrentQuota(final String key) {
        final RedisCommands<String, String> commands = redis.sync(); // zincirleme
        commands.multi();// multi birden fazla komutun zincirleneceğini belirtiriz
        commands.incrby(key, 1);// increase nokta arttırma birincisi anahtar ikincisi miktar
        var remainingSeconds = 60 - LocalTime.now().getSecond(); // 60 saniyeyi şimdiki zamandan çıkarıyoruz
        commands.expire(key, remainingSeconds); // expire her dakikanın sonunda komut süresi dolacak
        commands.exec(); // execute isteği redis örneğine yürütüyoruz
    }


}
