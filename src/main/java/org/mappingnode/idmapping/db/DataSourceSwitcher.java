package org.mappingnode.idmapping.db;


public class DataSourceSwitcher {

    @SuppressWarnings("rawtypes")
    private static final ThreadLocal contextHolder = new ThreadLocal();

    @SuppressWarnings("unchecked")
    public static void setDataSource(DBTypeEnum type) {
        contextHolder.set(type);
    }

    public static void setMaster() {
        setDataSource(DBTypeEnum.MASTER);
    }

    public static void setSlave() {
        setDataSource(DBTypeEnum.SLAVE);
    }

    public static DBTypeEnum getDataSource() {
        return (DBTypeEnum) contextHolder.get();
    }

    public static void clearDataSource() {
        contextHolder.remove();
    }
}
