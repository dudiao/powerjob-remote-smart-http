package tech.powerjob.dudiao.remote;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.smartboot.http.common.enums.HeaderValueEnum;
import org.smartboot.http.server.HttpRequest;
import org.smartboot.http.server.HttpResponse;
import org.smartboot.http.server.HttpServerHandler;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.remote.framework.actor.ActorInfo;
import tech.powerjob.remote.framework.actor.HandlerInfo;
import tech.powerjob.remote.framework.utils.RemoteUtils;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * @author songyinyin
 * @since 2023/9/19 11:48
 */
public class SmartHttpHandler extends HttpServerHandler {

    private final ActorInfo actorInfo;

    private final HandlerInfo handlerInfo;

    private final ObjectMapper objectMapper;

    public SmartHttpHandler(ActorInfo actorInfo, HandlerInfo handlerInfo) {
        this.actorInfo = actorInfo;
        this.handlerInfo = handlerInfo;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response) throws Throwable {
        Method method = handlerInfo.getMethod();
        Optional<Class<?>> powerSerializeClz = RemoteUtils.findPowerSerialize(method.getParameterTypes());

        // 内部框架，严格模式，绑定失败直接报错
        if (!powerSerializeClz.isPresent()) {
            throw new PowerJobException("can't find any 'PowerSerialize' object in handler args: " + handlerInfo.getLocation());
        }

        String body = IOUtils.toString(request.getInputStream(), StandardCharsets.UTF_8);
        Object params = objectMapper.readValue(body, powerSerializeClz.get());

        Object result = method.invoke(actorInfo.getActor(), params);
        if (result != null) {
            if (result instanceof String) {
                response.write(((String) result).getBytes());
            } else {
                response.setContentType(HeaderValueEnum.APPLICATION_JSON.getName());
                response.write(objectMapper.writeValueAsBytes(result));
            }
        }
    }
}
