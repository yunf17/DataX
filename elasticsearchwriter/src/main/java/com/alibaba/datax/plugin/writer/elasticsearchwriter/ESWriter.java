package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

public class ESWriter extends Writer {

    private final static String WRITE_COLUMNS = "write_columns";

    public static class Job extends Writer.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
            ESClient esClient = new ESClient();
            esClient.createClient(Key.getEndpoint(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf));

            String indexName = Key.getIndexName(conf);
            String typeName = Key.getTypeName(conf);
            boolean writePriority = Key.isWritePriority(conf);
            JSONObject mappings = genMappings(typeName);
            JSONObject settings = JSON.parseObject(JSONObject.toJSONString(Key.getSettings(conf)));
            log.info(String.format("index:[%s]", indexName));
            log.info(String.format("type:[%s]", typeName));
            log.info(String.format("settings:[%s]", settings));
            log.info(String.format("mappings:[%s]", mappings));

            try {
                boolean isIndicesExists = esClient.indicesExists(indexName);
                if (Key.isCleanup(this.conf) && isIndicesExists) {
                    esClient.deleteIndex(indexName);
                }
                // 强制创建,内部自动忽略已存在的情况
                if (!esClient.createIndex(indexName, settings, mappings)) {
                    throw new IOException("create index or mapping failed");
                }
                if (writePriority) {
                    LinkedHashMap<String, String> settingContentMap = new LinkedHashMap<>();
                    settingContentMap.put("refresh_interval", "300s");
                    settingContentMap.put("index.translog.durability", "async");
                    settingContentMap.put("index.translog.flush_threshold_size", "1024mb");

                    if (esClient.setIndexSettings(indexName, settingContentMap)) {
                        log.info("set write priority success");
                    } else {
                        log.warn("set write priority failed");
                    }
                }
            } catch (Exception ex) {
                throw DataXException.asDataXException(ESWriterErrorCode.ES_MAPPINGS, ex.toString());
            }
            esClient.closeRestHighLevelClient();
        }

        private JSONObject genMappings(String typeName) {
            JSONObject mappings = null;
            Map<String, Object> propMap = new HashMap<String, Object>();
            List<ESColumn> columnList = new ArrayList<ESColumn>();

            List column = conf.getList("column");
            if (column != null) {
                for (Object col : column) {
                    JSONObject jo = JSONObject.parseObject(col.toString());
                    String colName = jo.getString("name");
                    String colTypeStr = jo.getString("type");
                    if (colTypeStr == null) {
                        throw DataXException.asDataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, col.toString() + " " +
                                "column must have type");
                    }
                    ESFieldType colType = ESFieldType.getESFieldType(colTypeStr);
                    if (colType == null) {
                        throw DataXException.asDataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, col.toString() + " " +
                                "unsupported type");
                    }

                    ESColumn columnItem = new ESColumn();

                    if (colName.equals(Key.PRIMARY_KEY_COLUMN_NAME)) {
                        colType = ESFieldType.ID;
                        colTypeStr = "id";
                    }

                    columnItem.setName(colName);
                    columnItem.setType(colTypeStr);

                    if (colType == ESFieldType.ID) {
                        columnList.add(columnItem);
                        // 如果是id,则properties为空
                        continue;
                    }

                    Boolean array = jo.getBoolean("array");
                    if (array != null) {
                        columnItem.setArray(array);
                    }

                    jo.remove("name");
                    propMap.put(colName, jo);
                    columnList.add(columnItem);
                }
            }

            conf.set(WRITE_COLUMNS, JSON.toJSONString(columnList));

            Map<String, Object> rootMappings = new HashMap<String, Object>();
            //Map<String, Object> typeMappings = new HashMap<String, Object>();
            //typeMappings.put("dynamic", Key.getDynamic(conf));
            rootMappings.put("properties", propMap);
            //rootMappings.put(typeName, typeMappings);

            mappings = JSON.parseObject(JSON.toJSONString(rootMappings));

            if (mappings == null || mappings.keySet().size() == 0) {
                throw DataXException.asDataXException(ESWriterErrorCode.BAD_CONFIG_VALUE, "must have mappings");
            }

            return mappings;
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        @Override
        public void post() {
            ESClient esClient = new ESClient();
            esClient.createClient(Key.getEndpoint(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf));
            String alias = Key.getAlias(conf);
            if (StringUtils.isNotEmpty(alias)) {
                log.info(String.format("alias [%s] to [%s]", alias, Key.getIndexName(conf)));
                try {
                    esClient.alias(Key.getIndexName(conf), alias, Key.isNeedCleanAlias(conf));
                } catch (IOException e) {
                    throw DataXException.asDataXException(ESWriterErrorCode.ES_ALIAS_MODIFY, e);
                }
            }
            if (Key.isWritePriority(conf)) {
                LinkedHashMap<String, String> settingContentMap = new LinkedHashMap<>();
                settingContentMap.put("refresh_interval", "1s");
                settingContentMap.put("index.translog.durability", "request");
                settingContentMap.put("index.translog.flush_threshold_size", "512mb");

                try {
                    if (esClient.setIndexSettings(Key.getIndexName(conf), settingContentMap)) {
                        log.info("restore write priority success");
                    } else {
                        log.warn("restore write priority failed");
                    }
                } catch (IOException e) {
                    log.error("restore write priority failed: " + e);
                }
            }
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {

        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf;

        ESClient esClient = null;
        private List<ESFieldType> typeList;
        private List<ESColumn> columnList;

        private int trySize;
        private int batchSize;
        private String index;
        private String type;
        private String splitter;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            index = Key.getIndexName(conf);
            type = Key.getTypeName(conf);

            trySize = Key.getTrySize(conf);
            batchSize = Key.getBatchSize(conf);
            splitter = Key.getSplitter(conf);
            columnList = JSON.parseObject(this.conf.getString(WRITE_COLUMNS), new TypeReference<List<ESColumn>>() {
            });

            typeList = new ArrayList<ESFieldType>();

            for (ESColumn col : columnList) {
                typeList.add(ESFieldType.getESFieldType(col.getType()));
            }
        }

        @Override
        public void prepare() {
            esClient = new ESClient();
            esClient.createClient(Key.getEndpoint(conf),
                    Key.getAccessID(conf),
                    Key.getAccessKey(conf));
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
            Record record = null;
            long total = 0;
            while ((record = recordReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if (writerBuffer.size() >= this.batchSize) {
                    total += doBatchInsert(writerBuffer);
                    writerBuffer.clear();
                }
            }

            if (!writerBuffer.isEmpty()) {
                total += doBatchInsert(writerBuffer);
                writerBuffer.clear();
            }

            String msg = String.format("task end, write size :%d", total);
            getTaskPluginCollector().collectMessage("writesize", String.valueOf(total));
            log.info(msg);
            esClient.closeRestHighLevelClient();
        }

        private String getDateStr(ESColumn esColumn, Column column) {
            DateTime date = null;
            DateTimeZone dtz = DateTimeZone.getDefault();
            if (esColumn.getTimezone() != null) {
                // 所有时区参考 http://www.joda.org/joda-time/timezones.html
                dtz = DateTimeZone.forID(esColumn.getTimezone());
            }
            if (column.getType() != Column.Type.DATE && esColumn.getFormat() != null) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern(esColumn.getFormat());
                date = formatter.withZone(dtz).parseDateTime(column.asString());
                return date.toString();
            } else if (column.getType() == Column.Type.DATE) {
                date = new DateTime(column.asLong(), dtz);
                return date.toString();
            } else {
                return column.asString();
            }
        }

        private long doBatchInsert(final List<Record> writerBuffer) {
            JSONObject data;
            //StringBuffer sb = new StringBuffer();
            List<DocumentData> doucumentDataList = new ArrayList<>();

            if (writerBuffer.size() > 0) {
                DocumentData dd;
                String id;
                for (Record record : writerBuffer) {
                    dd = new DocumentData();
                    data = new JSONObject();
                    id = null;

                    Column column;
                    String columnName;
                    ESFieldType columnType;
                    String[] dataList;
                    for (int i = 0; i < record.getColumnNumber(); i++) {
                        column = record.getColumn(i);
                        columnName = columnList.get(i).getName();
                        columnType = typeList.get(i);
                        //如果是数组类型，那它传入的必是字符串类型
                        if (columnList.get(i).isArray() != null && columnList.get(i).isArray()) {
                            dataList = column.asString().split(splitter);
                            if (!columnType.equals(ESFieldType.DATE)) {
                                data.put(columnName, dataList);
                            } else {
                                for (int pos = 0; pos < dataList.length; pos++) {
                                    dataList[pos] = getDateStr(columnList.get(i), column);
                                }
                                data.put(columnName, dataList);
                            }
                        } else {
                            switch (columnType) {
                                case ID:
                                    if (id != null) {
                                        id += record.getColumn(i).asString();
                                    } else {
                                        id = record.getColumn(i).asString();
                                    }
                                    break;
                                case DATE:
                                    try {
                                        String dateStr = getDateStr(columnList.get(i), column);
                                        data.put(columnName, dateStr);
                                    } catch (Exception e) {
                                        getTaskPluginCollector().collectDirtyRecord(record, String.format("时间类型解析失败 " +
                                                "[%s:%s] exception: %s", columnName, column.toString(), e.toString()));
                                    }
                                    break;
                                case KEYWORD:
                                case STRING:
                                case TEXT:
                                case IP:
                                case GEO_POINT:
                                    data.put(columnName, column.asString());
                                    break;
                                case BOOLEAN:
                                    data.put(columnName, column.asBoolean());
                                    break;
                                case BYTE:
                                case BINARY:
                                    data.put(columnName, column.asBytes());
                                    break;
                                case LONG:
                                    data.put(columnName, column.asLong());
                                    break;
                                case INTEGER:
                                    data.put(columnName, column.asBigInteger());
                                    break;
                                case SHORT:
                                    data.put(columnName, column.asBigInteger());
                                    break;
                                case FLOAT:
                                case DOUBLE:
                                    data.put(columnName, column.asDouble());
                                    break;
                                case NESTED:
                                case OBJECT:
                                case GEO_SHAPE:
                                    data.put(columnName, JSON.parse(column.asString()));
                                    break;
                                default:
                                    getTaskPluginCollector().collectDirtyRecord(record,
                                            "类型错误:不支持的类型:" + columnType + " " + columnName);
                            }
                        }
                    }

                    /*if (id == null) {
                        sb.append("{\"index\":{}}");
                    } else {
                        sb.append("{\"index\":{\"_id\":\"" + id + "\"}}");
                    }
                    sb.append("\r\n");
                    sb.append(data);
                    sb.append("\r\n");*/

                    dd.setId(id);
                    dd.setData(data.toJSONString());
                    doucumentDataList.add(dd);
                }
            }

            try {
                return RetryUtil.executeWithRetry(new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        //JSONObject result = null;
                        CommonResponse result = null;
                        try {
                            result = esClient.batchInsert(index, type, doucumentDataList);
                            /*if (!result.isSuccess()) {
                                log.error(String.format("batch insert data to es failed :[%s]", result.getErrorMsg()));
                                throw DataXException.asDataXException(ESWriterErrorCode.ES_INDEX_INSERT,
                                        result.getErrorMsg());
                            }*/

                            if (!result.isSuccess()) {

                                BulkItemResponse[] responseItems = result.getResponseItems();
                                int failSize = 0;

                                for (int i = 0; i < responseItems.length; i++) {
                                    BulkItemResponse item = responseItems[i];
                                    if (item.isFailed()) {
                                        failSize++;
                                        getTaskPluginCollector().collectDirtyRecord(writerBuffer.get(i),
                                                String.format("error: %s", item.getFailureMessage()));
                                    }

                                }
                                log.error(String.format("batch insert data to es failed :[%s]", result.getErrorMsg()));
                                return writerBuffer.size() - failSize;
                            } else {
                                return writerBuffer.size();
                            }
                        } catch (IOException e) {
                           /* if (null != result) {
                                int status = result.getIntValue("status");
                                switch (status) {
                                    case 429: //TOO_MANY_REQUESTS
                                        log.warn("server response too many requests, so auto reduce speed");
                                        break;
                                }
                            }*/
                            log.error(String.format("batch insert data to es failed :[%s]", e.getMessage()));
                            throw DataXException.asDataXException(ESWriterErrorCode.ES_INDEX_INSERT, e.getMessage());
                        }
                    }
                }, trySize, 60000L, true);
            } catch (Exception e) {
                if (Key.isIgnoreWriteError(this.conf)) {
                    log.warn(String.format("重试[%d]次写入失败，忽略该错误，继续写入!", trySize));
                } else {
                    throw DataXException.asDataXException(ESWriterErrorCode.ES_INDEX_INSERT, e);
                }
            }
            return 0;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
            esClient.closeRestHighLevelClient();
        }
    }
}
