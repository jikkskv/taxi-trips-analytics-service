package com.xyztaxicompany.analytics.trips.exception;

public class BizException extends RuntimeException {

    BizErrorCodeEnum errorCode;

    public BizException(BizErrorCodeEnum errorCode) {
        super();
        this.errorCode = errorCode;
    }

    public BizErrorCodeEnum getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorCode.getMessage();
    }
}
