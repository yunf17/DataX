package com.alibaba.datax.plugin.writer.elasticsearchwriter;

/**
 * @Author:yunf
 * @Description:
 * @Date:2022/5/9 14:52
 * @Version: 1.0
 */
public class DocumentData {

    private String id;

    private String data;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
