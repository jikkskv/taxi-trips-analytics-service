package com.xyztaxicompany.analytics.trips.exception;

import lombok.Getter;

@Getter
public enum BizErrorCodeEnum implements ErrorCode {
    NO_ERROR(0, "Success"),
    SYSTEM_ERROR(500, "System Error"),
    BAD_DATA(300, "Bad Data or Bad input");

    private int code;
    private String message;

    BizErrorCodeEnum(final int code) {
        this.code = code;
    }

    BizErrorCodeEnum(final int code, final String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String toString() {
        return "[" + this.getCode() + "]" + this.getMessage();
    }
}
