package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import org.elasticsearch.action.bulk.BulkItemResponse;

/**
 * @Author:yunf
 * @Description:
 * @Date:2022/5/9 15:05
 * @Version: 1.0
 */
public class CommonResponse {

    private boolean isSuccess;

    private String errorMsg;

    private BulkItemResponse[] responseItems;

    public BulkItemResponse[] getResponseItems() {
        return responseItems;
    }

    public void setResponseItems(BulkItemResponse[] responseItems) {
        this.responseItems = responseItems;
    }

    public CommonResponse() {

    }
    public CommonResponse(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean success) {
        isSuccess = success;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
}
