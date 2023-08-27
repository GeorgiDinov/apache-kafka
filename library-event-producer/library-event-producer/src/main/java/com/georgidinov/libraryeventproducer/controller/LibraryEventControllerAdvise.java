package com.georgidinov.libraryeventproducer.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class LibraryEventControllerAdvise {

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ErrorMessage handleValidationException(MethodArgumentNotValidException exception) {
        String errorResult = exception.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(fieldError -> fieldError.getField() + " - " + fieldError.getDefaultMessage())
                .sorted()
                .collect(Collectors.joining(", "));
        log.error("Error message = {}", errorResult);
        return new ErrorMessage(errorResult);
    }


    public static class ErrorMessage {

        @JsonProperty("error_message")
        private String message;


        public ErrorMessage(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }

}
