package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Key {
    // ----------------------------------------
    //  类型定义 主键字段定义
    // ----------------------------------------

    public static final String SEARCH_KEY = "search";

   /* public static SearchType getSearchType(Configuration conf) {
        String searchType = conf.getString("searchType", SearchType.DFS_QUERY_THEN_FETCH.toString());
        return SearchType.valueOf(searchType.toUpperCase());
    }*/

    public static String getEndpoints(Configuration conf) {
        return conf.getNecessaryValue("endpoints", ESReaderErrorCode.BAD_CONFIG_VALUE);
    }

    public static String getAccessID(Configuration conf) {
        return conf.getString("accessId", "");
    }

    public static String getAccessKey(Configuration conf) {
        return conf.getString("accessKey", "");
    }

    public static int getSize(Configuration conf) {
        return conf.getInt("size", 10);
    }

    public static int getTrySize(Configuration conf) {
        return conf.getInt("trySize", 30);
    }

    public static int getTimeout(Configuration conf) {
        return conf.getInt("timeout", 60000);
    }

    public static boolean isCleanup(Configuration conf) {
        return conf.getBool("cleanup", false);
    }

    public static boolean isDiscovery(Configuration conf) {
        return conf.getBool("discovery", false);
    }

    public static boolean isCompression(Configuration conf) {
        return conf.getBool("compression", true);
    }

    public static boolean isMultiThread(Configuration conf) {
        return conf.getBool("multiThread", true);
    }

    public static String getIndexName(Configuration conf) {
        return conf.getNecessaryValue("index", ESReaderErrorCode.BAD_CONFIG_VALUE);
    }

    public static String getTypeName(Configuration conf) {
        String indexType = conf.getString("indexType");
        if (StringUtils.isBlank(indexType)) {
            indexType = conf.getString("type", getIndexName(conf));
        }
        return indexType;
    }


    public static boolean isIgnoreWriteError(Configuration conf) {
        return conf.getBool("ignoreWriteError", false);
    }

    public static boolean isIgnoreParseError(Configuration conf) {
        return conf.getBool("ignoreParseError", true);
    }


    public static boolean isHighSpeedMode(Configuration conf) {
        if ("highspeed".equals(conf.getString("mode", ""))) {
            return true;
        }
        return false;
    }

    public static String getAlias(Configuration conf) {
        return conf.getString("alias", "");
    }

    public static boolean isNeedCleanAlias(Configuration conf) {
        String mode = conf.getString("aliasMode", "append");
        if ("exclusive".equals(mode)) {
            return true;
        }
        return false;
    }

    public static Map<String, Object> getSettings(Configuration conf) {
        return conf.getMap("settings", new HashMap<String, Object>());
    }

    public static Map<String, Object> getHeaders(Configuration conf) {
        return conf.getMap("headers", new HashMap<>());
    }

    public static String getQuery(Configuration conf) {
        return conf.getConfiguration(Key.SEARCH_KEY).toString();
    }

    public static String getSplitter(Configuration conf) {
        return conf.getString("splitter", "-,-");
    }

    public static boolean getDynamic(Configuration conf) {
        return conf.getBool("dynamic", false);
    }

    public static String[] getIncludes(Configuration conf) {
        List<String> includes = conf.getList("includes", String.class);
        if (null != includes) {
            return includes.toArray(new String[0]);
        }
        return null;
    }

    public static String[] getExcludes(Configuration conf) {
        List<String> excludes = conf.getList("excludes", String.class);
        if (null != excludes) {
            return excludes.toArray(new String[0]);
        }
        return null;
    }

    public static boolean getContainsId(Configuration conf) {
        return conf.getBool("containsId", false);
    }
}