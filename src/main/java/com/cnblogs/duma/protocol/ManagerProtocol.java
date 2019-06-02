package com.cnblogs.duma.protocol;

public interface ManagerProtocol {

    /**
     * 设置支持的最大表的个数
     * @param tableNum 表数量
     * @return 设置成功返回 true，设置失败返回 false
     * todo 需要处理抛出的异常
     */
    public boolean setMaxTable(int tableNum);
}
