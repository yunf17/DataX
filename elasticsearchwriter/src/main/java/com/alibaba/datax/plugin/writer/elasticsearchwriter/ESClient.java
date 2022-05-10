package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.DeleteAliasRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by lizhenchao on 2020/12/29.
 * <p>
 * modified by yunf on 2022.05.07
 */
public class ESClient {

    private static final Logger log = LoggerFactory.getLogger(ESClient.class);

    private RestHighLevelClient client = null;

    public RestHighLevelClient getClient() {
        return client;
    }

    public void createClient(String endpoints,
                             String username,
                             String password) {
        String[] endpointSplit = endpoints.split(",");
        HttpHost[] hosts = new HttpHost[endpointSplit.length];
        for (int i = 0; i < endpointSplit.length; i++) {
            String[] ips = endpointSplit[0].split(":");
            String ip = ips[0];
            int port = Integer.parseInt(ips[1]);
            hosts[i] = new HttpHost(ip, port, HttpHost.DEFAULT_SCHEME_NAME);
        }
        RestClientBuilder builder = RestClient.builder(hosts);
        // 账号密码认证
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            final BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
            basicCredentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(f -> f.setDefaultCredentialsProvider(basicCredentialsProvider));
        }
        client = new RestHighLevelClient(builder);
        log.info("======= RestHighLevelClient 初始化成功 =======");
    }

    /**
     * 判断索引是否存在
     *
     * @param indexName
     * @return
     * @throws Exception
     */
    public boolean indicesExists(String indexName) throws Exception {
        GetIndexRequest request = new GetIndexRequest(indexName);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除索引
     *
     * @param indexName
     * @return
     * @throws Exception
     */
    public boolean deleteIndex(String indexName) throws Exception {
        log.info("delete index " + indexName);
        if (indicesExists(indexName)) {
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);

            AcknowledgedResponse acknowledgedResponse = client.indices().delete(request, RequestOptions.DEFAULT);

            if (acknowledgedResponse.isAcknowledged()) {
                return true;
            } else {
                return false;
            }
        } else {
            log.info("index cannot found, skip delete " + indexName);
            return true;
        }
    }

    /*public JSONObject sendRequestToOrigin(String method, String endpoint, String requestBody) {
        HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
        Request request = new Request(method, endpoint);
        request.setEntity(entity);
        try {
            Response response = restClient.performRequest(request);
            return JSON.parseObject(EntityUtils.toString(response.getEntity()));
        } catch (ResponseException e) {
            HttpEntity en = ((ResponseException) e).getResponse().getEntity();
            try {
                return JSON.parseObject(EntityUtils.toString(en));
            } catch (IOException ex) {
                ex.printStackTrace();
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public JSONObject sendRequestToOrigin2(String method, String endpoint, String requestBody) throws IOException {
        HttpEntity entity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
        Request request = new Request(method, endpoint);
        request.setEntity(entity);
        try {
            Response response = restClient.performRequest(request);
            return JSON.parseObject(EntityUtils.toString(response.getEntity()));
        } catch (ResponseException e) {
            HttpEntity en = ((ResponseException) e).getResponse().getEntity();
            return JSON.parseObject(EntityUtils.toString(en));
        }
    }*/

    /**
     * 创建索引
     *
     * @param indexName
     * @param settings
     * @param mappings
     * @return
     * @throws Exception
     */
    public boolean createIndex(String indexName, JSONObject settings, JSONObject mappings) throws Exception {
        if (!indicesExists(indexName)) {
            CreateIndexRequest request = new CreateIndexRequest(indexName);

            // settings
            request.settings(settings.toJSONString(), XContentType.JSON);

            // mappings
            XContentBuilder builder = JsonXContent.contentBuilder().map(mappings);
            request.mapping(builder);

            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

            if (createIndexResponse.isAcknowledged()) {
                log.info(String.format("create [%s] index success", indexName));
                return true;
            } else {
                log.error(String.format("create [%s] index failed", indexName));
                return false;
            }
        } else {
            log.info(String.format("index [%s] already exists", indexName));
            return true;
        }
    }

    /**
     * 索引settings设置
     *
     * @param indexName
     * @param settingContent
     * @return
     */
    public boolean setIndexSettings(String indexName, Map<String, String> settingContent) throws IOException {

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName);

        Settings.Builder builder = Settings.builder();

        for (String key : settingContent.keySet()) {
            builder.put(key, settingContent.get(key));
        }

        updateSettingsRequest.settings(builder);

        AcknowledgedResponse acknowledgedResponse = client.indices().putSettings(updateSettingsRequest,
                RequestOptions.DEFAULT);

        return acknowledgedResponse.isAcknowledged();

    }

    /**
     * 判断指定索引的别名是否存在
     *
     * @param indexName
     * @return
     */
    public boolean aliasExists(String indexName) throws IOException {
        GetAliasesRequest request = new GetAliasesRequest();
        request.indices(indexName);

        GetAliasesResponse response = client.indices().getAlias(request, RequestOptions.DEFAULT);
        Map<String, Set<AliasMetadata>> aliases = response.getAliases();

        Set<AliasMetadata> aliasMetadataSet = aliases.get(indexName);

        if (null != aliasMetadataSet && !aliasMetadataSet.isEmpty()) {
            return true;
        }

        return false;
    }

    /**
     * 获取索引别名列表
     *
     * @param indexName
     * @return
     * @throws IOException
     */
    public Set getAlias(String indexName) throws IOException {

        if (aliasExists(indexName)) {
            GetAliasesRequest request = new GetAliasesRequest();
            request.indices(indexName);

            GetAliasesResponse response = client.indices().getAlias(request, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetadata>> aliases = response.getAliases();

            Set<AliasMetadata> aliasMetadataSet = aliases.get(indexName);

            return aliasMetadataSet.stream().map(t -> ((AliasMetadata) t).getAlias()).collect(Collectors.toSet());
        } else {
            log.info(String.format("index [%s] has no alias", indexName));
            return null;
        }
    }

    /**
     * 创建别名
     *
     * @param indexName
     * @param aliasName
     */
    public boolean createAlias(String indexName, String aliasName) throws IOException {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(indexName).alias(
                aliasName));

        AcknowledgedResponse acknowledgedResponse = client.indices().updateAliases(indicesAliasesRequest
                , RequestOptions.DEFAULT);

        if (acknowledgedResponse.isAcknowledged()) {
            return true;
        }

        return false;
    }

    /**
     * 删除别名
     *
     * @param indexName
     * @param aliasName
     * @return
     */
    public boolean deleteAlias(String indexName, String aliasName) throws IOException {
        DeleteAliasRequest deleteAliasRequest = new DeleteAliasRequest(indexName, aliasName);

        org.elasticsearch.client.core.AcknowledgedResponse acknowledgedResponse =
                client.indices().deleteAlias(deleteAliasRequest,
                        RequestOptions.DEFAULT);

        if (acknowledgedResponse.isAcknowledged()) {
            return true;
        }
        return false;
    }

    /**
     * 索引别名相关操作
     *
     * @param indexName
     * @param aliasName
     * @param needClean
     * @return
     * @throws IOException
     */
    public boolean alias(String indexName, String aliasName, boolean needClean) throws IOException {
        if (needClean) {
            Set<String> aliases = getAlias(indexName);
            if (null != aliases && aliases.size() > 0) {
                for (String alias : aliases) {
                    if (deleteAlias(indexName, alias)) {
                        log.info("index {} remove alias {} success", indexName, alias);
                    } else {
                        log.warn("index {} remove alias {} failed", indexName, alias);
                    }
                }
            }
            return createAlias(indexName, aliasName);
        } else {
            return createAlias(indexName, aliasName);
        }
    }

    /**
     * 批量插入
     *
     * @param index
     * @param type
     * @param dataList
     * @return
     */
    public CommonResponse batchInsert(String index, String type, List<DocumentData> dataList) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();

        IndexRequest indexRequest = null;
        for (DocumentData documentData : dataList) {
            indexRequest = new IndexRequest(index, type);

            if (StringUtils.isNotEmpty(documentData.getId())) {
                indexRequest.id(documentData.getId());
            }
            bulkRequest.add(indexRequest.source(documentData.getData(), XContentType.JSON));
        }

        // 客户端批量执行
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setSuccess(!bulkResponse.hasFailures());
        commonResponse.setResponseItems(bulkResponse.getItems());

        if (bulkResponse.hasFailures()) {
            commonResponse.setErrorMsg(bulkResponse.buildFailureMessage());
        }

        return commonResponse;
    }


    /**
     * 关闭RestHighLevelClient客户端
     */
    public void closeRestHighLevelClient() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                log.error("close RestHighLevelClient is error: {}", e.getMessage());
            }
        }
    }
}
