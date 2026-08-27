package com.ridehailing.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;

public class JsonUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static ObjectMapper getMapper() {
        return MAPPER;
    }

    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
}
