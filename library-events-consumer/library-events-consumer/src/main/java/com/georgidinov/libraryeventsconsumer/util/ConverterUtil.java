package com.georgidinov.libraryeventsconsumer.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;

public final class ConverterUtil {


    private static final ObjectMapper objectMapper = new ObjectMapper();;

    private ConverterUtil() {
    }

    public static <T> String getObjectAsString(T object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }

    public static <T> T getObjectFromString (String objectData, Class<T> classType) throws JsonProcessingException {
        return objectMapper.readValue(objectData, classType);
    }

    public static <T> T getObjectFromClassPath (ClassPathResource resource, Class<T> classType) throws IOException {
        return objectMapper.readValue(resource.getFile(), classType);
    }

}
