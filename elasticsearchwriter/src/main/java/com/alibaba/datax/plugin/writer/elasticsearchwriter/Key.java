package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import com.alibaba.datax.common.util.Configuration;

import java.util.HashMap;
import java.util.Map;

public final class Key {
    // ----------------------------------------
    //  类型定义 主键字段定义
    // ----------------------------------------
    public static final String PRIMARY_KEY_COLUMN_NAME = "pk";

    public enum ActionType {
        UNKONW,
        INDEX,
        CREATE,
        DELETE,
        UPDATE
    }

    public static ActionType getActionType(Configuration conf) {
        String actionType = conf.getString("actionType", "index");
        if ("index".equals(actionType)) {
            return ActionType.INDEX;
        } else if ("create".equals(actionType)) {
            return ActionType.CREATE;
        } else if ("delete".equals(actionType)) {
            return ActionType.DELETE;
        } else if ("update".equals(actionType)) {
            return ActionType.UPDATE;
        } else {
            return ActionType.UNKONW;
        }
    }

    public static String getEndpoint(Configuration conf) {
        return conf.getNecessaryValue("endpoints", ESWriterErrorCode.BAD_CONFIG_VALUE);
    }

    public static String getAccessID(Configuration conf) {
        return conf.getString("accessId", "");
    }

    public static String getAccessKey(Configuration conf) {
        return conf.getString("accessKey", "");
    }

    public static int getBatchSize(Configuration conf) {
        return conf.getInt("batchSize", 1024);
    }

    public static int getTrySize(Configuration conf) {
        return conf.getInt("trySize", 30);
    }

    public static int getTimeout(Configuration conf) {
        return  conf.getInt("timeout", 600000);
    }

    public static boolean isCleanup(Configuration conf) {
        return conf.getBool("cleanup", false);
    }

//    public static boolean isCompression(Configuration conf) {
//        return conf.getBool("compression", true);
//    }

    public static String getIndexName(Configuration conf) {
        return conf.getNecessaryValue("index", ESWriterErrorCode.BAD_CONFIG_VALUE);
    }

    public static String getTypeName(Configuration conf) {
        return conf.getString("type", "_doc");
    }


    public static boolean isIgnoreWriteError(Configuration conf) {
        return conf.getBool("ignoreWriteError", false);
    }

    public static boolean isIgnoreParseError(Configuration conf) {
        return conf.getBool("ignoreParseError", true);
    }

    public static boolean isWritePriority(Configuration conf) {
        return conf.getBool("writePriority", false);
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

    public static Map<String, Object> getColumn(Configuration conf) {
        return conf.getMap("column", new HashMap<String, Object>());
    }

    public static String getSplitter(Configuration conf) {
        return conf.getString("splitter", "-,-");
    }

    public static String getDynamic(Configuration conf) {
        return conf.getString("dynamic", "strict");
    }
}
