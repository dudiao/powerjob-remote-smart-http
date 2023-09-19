package tech.powerjob.dudiao.remote;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.smartboot.http.server.HttpBootstrap;
import org.smartboot.http.server.HttpServerConfiguration;
import org.smartboot.http.server.handler.HttpRouteHandler;
import tech.powerjob.common.enums.Protocol;
import tech.powerjob.common.utils.CollectionUtils;
import tech.powerjob.remote.framework.actor.ActorInfo;
import tech.powerjob.remote.framework.actor.HandlerInfo;
import tech.powerjob.remote.framework.cs.CSInitializer;
import tech.powerjob.remote.framework.cs.CSInitializerConfig;
import tech.powerjob.remote.framework.transporter.Transporter;

import java.io.IOException;
import java.util.List;

/**
 * @author songyinyin
 * @since 2023/9/12 16:51
 */
@Slf4j
public class SmartHttpCSInitializer implements CSInitializer {

    private HttpBootstrap server = null;

    private CSInitializerConfig config;


    @Override
    public String type() {
        return Protocol.HTTP.name();
    }

    @Override
    public void init(CSInitializerConfig csInitializerConfig) {
        this.config = csInitializerConfig;

        server = new HttpBootstrap();
        HttpServerConfiguration configuration = server.configuration();

        if (StringUtils.isNotBlank(csInitializerConfig.getBindAddress().getHost())) {
            configuration.host(csInitializerConfig.getBindAddress().getHost());
        }
        configuration.serverName("powerjob-" + csInitializerConfig.getServerType().name().toLowerCase());
        server.setPort(csInitializerConfig.getBindAddress().getPort());
        server.start();

    }

    @Override
    public Transporter buildTransporter() {
        return new SmartHttpTransporter(config);
    }

    @Override
    public void bindHandlers(List<ActorInfo> list) {
        HttpRouteHandler routeHandler = new HttpRouteHandler();
        for (ActorInfo actorInfo : list) {
            if (CollectionUtils.isEmpty(actorInfo.getHandlerInfos())) {
                continue;
            }
            for (HandlerInfo handlerInfo : actorInfo.getHandlerInfos()) {
                routeHandler.route(handlerInfo.getLocation().toPath(), new SmartHttpHandler(actorInfo, handlerInfo));
            }
        }
        server.httpHandler(routeHandler);
        log.info("[PowerJobRemoteEngine] startup smart HttpServer successfully!");
    }

    @Override
    public void close() throws IOException {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }
}
