package tech.powerjob.dudiao.remote;

import tech.powerjob.remote.framework.transporter.Protocol;

/**
 * @author songyinyin
 * @since 2023/9/12 16:50
 */
public class HttpProtocol implements Protocol {

    @Override
    public String name() {
        return tech.powerjob.common.enums.Protocol.HTTP.name();
    }
}
