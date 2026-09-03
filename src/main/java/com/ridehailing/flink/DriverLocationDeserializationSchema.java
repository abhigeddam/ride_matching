package com.ridehailing.flink;

import com.ridehailing.model.DriverLocationPing;
import com.ridehailing.util.JsonUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DriverLocationDeserializationSchema implements DeserializationSchema<DriverLocationPing> {
    private static final long serialVersionUID = 1L;

    @Override
    public DriverLocationPing deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        try {
            String json = new String(message, StandardCharsets.UTF_8);
            return JsonUtil.fromJson(json, DriverLocationPing.class);
        } catch (Exception e) {
            // Drop malformed messages gracefully
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(DriverLocationPing nextElement) {
        return false;
    }

    @Override
    public TypeInformation<DriverLocationPing> getProducedType() {
        return TypeInformation.of(DriverLocationPing.class);
    }
}
