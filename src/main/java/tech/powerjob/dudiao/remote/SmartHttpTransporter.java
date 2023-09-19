package tech.powerjob.dudiao.remote;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.smartboot.http.client.HttpClient;
import org.smartboot.http.client.HttpPost;
import org.smartboot.http.client.HttpResponse;
import org.smartboot.http.common.enums.HeaderValueEnum;
import org.smartboot.http.common.enums.HttpStatus;
import tech.powerjob.common.PowerJobDKey;
import tech.powerjob.common.PowerSerializable;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.remote.framework.base.RemotingException;
import tech.powerjob.remote.framework.base.ServerType;
import tech.powerjob.remote.framework.base.URL;
import tech.powerjob.remote.framework.cs.CSInitializerConfig;
import tech.powerjob.remote.framework.transporter.Protocol;
import tech.powerjob.remote.framework.transporter.Transporter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author songyinyin
 * @since 2023/9/12 16:54
 */
public class SmartHttpTransporter implements Transporter {

    private Cache<String, HttpClient> httpClientMap;

    private static final int CONNECTION_TIMEOUT_MS = 3000;

    /**
     * 默认开启长连接，且 75S 超时
     */
    private static final int DEFAULT_KEEP_ALIVE_TIMEOUT = 75;

    private final int keepaliveTimeout;

    private static final Protocol PROTOCOL = new HttpProtocol();

    private final ObjectMapper objectMapper;

    public SmartHttpTransporter(CSInitializerConfig config) {
        int maximumSize;
        if (ServerType.SERVER.equals(config.getServerType())) {
            maximumSize = 1000;
        } else {
            maximumSize = 100;
        }
        httpClientMap = CacheBuilder.newBuilder()
            .concurrencyLevel(Runtime.getRuntime().availableProcessors() * 4)
            .maximumSize(maximumSize)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build();

        String keepaliveTimeoutStr = System.getProperty(PowerJobDKey.TRANSPORTER_KEEP_ALIVE_TIMEOUT, String.valueOf(DEFAULT_KEEP_ALIVE_TIMEOUT));
        this.keepaliveTimeout = Integer.parseInt(keepaliveTimeoutStr);

        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Protocol getProtocol() {
        return PROTOCOL;
    }

    @Override
    public void tell(URL url, PowerSerializable powerSerializable) {
        post(url, powerSerializable, null);
    }

    @Override
    public <T> CompletionStage<T> ask(URL url, PowerSerializable powerSerializable, Class<T> clz) throws RemotingException {
        return post(url, powerSerializable, clz);
    }

    public <T> CompletionStage<T> post(URL url, PowerSerializable powerSerializable, Class<T> clz) throws RemotingException {
        try {
            HttpClient httpClient = httpClientMap.get(url.getAddress().toFullAddress(), () -> {
                HttpClient tem = new HttpClient(url.getAddress().getHost(), url.getAddress().getPort());
                tem.configuration().connectTimeout(CONNECTION_TIMEOUT_MS);
                return tem;
            });
            HttpPost post = httpClient.post(url.getLocation().toPath());
            post.header().keepalive(keepaliveTimeout > 0).setContentType(HeaderValueEnum.APPLICATION_JSON.getName());
            post.body().write(objectMapper.writeValueAsBytes(powerSerializable));
            HttpResponse httpResponse = post.done().get();
            final int statusCode = httpResponse.getStatus();
            if (statusCode != HttpStatus.OK.value()) {
                // CompletableFuture.get() 时会传递抛出该异常
                throw new RemotingException(String.format("request [host:%s,port:%s,url:%s] failed, status: %d, msg: %s",
                    url.getAddress().getHost(), url.getAddress().getPort(), url.getLocation().toPath(), statusCode, httpResponse.body()));
            }
            String body = httpResponse.body();
            if (clz == null) {
                return CompletableFuture.completedFuture(null);
            }
            if (clz.equals(String.class)) {
                return CompletableFuture.completedFuture((T) body);
            }
            return CompletableFuture.completedFuture(objectMapper.readValue(body, clz));

        } catch (ExecutionException | JsonProcessingException | InterruptedException e) {
            throw new PowerJobException(e);
        }
    }
}
