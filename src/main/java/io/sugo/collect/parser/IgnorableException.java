package io.sugo.collect.parser;

public class IgnorableException extends Exception {
    public IgnorableException() {
        this("");
    }

    public IgnorableException(String message) {
        super(message);
    }
}
