package org.example.dm.service;

import org.example.config.ConfigManager;
import org.example.dm.dao.DmLocalDao;
import org.example.service.JiandaoyunApiService;
import org.example.util.LogUtil;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DM数据推送到简道云服务
 * 负责将本地dm_order数据推送到简道云
 * 
 * 优化点：
 * 1. 利用sync_operation字段直接判断新增/更新，无需查询简道云
 * 2. 利用jdy_data_id字段存储简道云返回的数据ID
 * 3. 批量处理提高效率
 */
public class DmJdySyncService {
    private static DmJdySyncService instance;
    private final JiandaoyunApiService apiService;
    private final DmLocalDao localDao;
    private final ConfigManager configManager;
    
    // 配置常量
    private final String APP_ID;
    private final String ENTRY_ID;
    private final int MAX_RETRY;
    private final long RETRY_INTERVAL;
    private final int MAX_BATCH_SIZE;
    
    private DmJdySyncService() {
        this.apiService = JiandaoyunApiService.getInstance();
        this.localDao = DmLocalDao.getInstance();
        this.configManager = ConfigManager.getInstance();
        
        // 初始化配置
        this.APP_ID = configManager.getProperty("dm.jdy.appId");
        this.ENTRY_ID = configManager.getProperty("dm.jdy.entryId");
        this.MAX_RETRY = Integer.parseInt(configManager.getProperty("sync.maxRetry", "10"));
        this.RETRY_INTERVAL = Long.parseLong(configManager.getProperty("sync.retryInterval", "5000"));
        this.MAX_BATCH_SIZE = Integer.parseInt(configManager.getProperty("sync.maxBatchSize", "50"));
    }
    
    public static synchronized DmJdySyncService getInstance() {
        if (instance == null) {
            instance = new DmJdySyncService();
        }
        return instance;
    }
    
    /**
     * 执行DM数据推送到简道云
     */
    public void pushDataToJiandaoyun() {
        try {
            // 1. 查询待同步的订单
            List<org.example.dm.model.DmOrder> pendingOrders = localDao.queryPendingOrders();
            
            if (pendingOrders.isEmpty()) {
                LogUtil.logInfo("[DM推送] 无待同步数据");
                return;
            }
            
            // 有数据时才输出详细日志
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            System.out.println("\n=== DM数据推送简道云开始 " + timestamp + " ===");
            
            System.out.println("查询到 " + pendingOrders.size() + " 条待同步的DM订单");
            LogUtil.logInfo("查询到 " + pendingOrders.size() + " 条待同步的DM订单");
            
            // 2. 按sync_operation分组
            List<org.example.dm.model.DmOrder> createOrders = new ArrayList<>();
            List<org.example.dm.model.DmOrder> updateOrders = new ArrayList<>();
            
            for (org.example.dm.model.DmOrder order : pendingOrders) {
                if ("C".equals(order.getSyncOperation())) {
                    createOrders.add(order);
                } else if ("U".equals(order.getSyncOperation())) {
                    updateOrders.add(order);
                }
            }
            
            System.out.println("待创建: " + createOrders.size() + " 条, 待更新: " + updateOrders.size() + " 条");
            
            // 3. 批量创建新记录
            int createSuccess = batchCreateOrders(createOrders);
            System.out.println("批量创建完成: 成功 " + createSuccess + " 条");
            
            // 4. 逐个更新已存在记录
            int updateSuccess = updateOrders(updateOrders);
            System.out.println("更新完成: 成功 " + updateSuccess + " 条");
            
            System.out.println("=== DM数据推送简道云完成 ===");
            LogUtil.logInfo("DM数据推送简道云完成: 创建 " + createSuccess + " 条, 更新 " + updateSuccess + " 条");
            
        } catch (Exception e) {
            LogUtil.logError("DM数据推送简道云异常: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 批量创建订单
     */
    private int batchCreateOrders(List<org.example.dm.model.DmOrder> orders) {
        if (orders.isEmpty()) {
            return 0;
        }
        
        int successCount = 0;
        DmDataTransformService transformService = DmDataTransformService.getInstance();
        
        // 分批处理，每批最多MAX_BATCH_SIZE条
        for (int i = 0; i < orders.size(); i += MAX_BATCH_SIZE) {
            int endIndex = Math.min(i + MAX_BATCH_SIZE, orders.size());
            List<org.example.dm.model.DmOrder> batch = orders.subList(i, endIndex);
            
            List<Map<String, Object>> dataList = new ArrayList<>();
            List<org.example.dm.model.DmOrder> validOrders = new ArrayList<>();
            
            // 转换数据
            for (org.example.dm.model.DmOrder order : batch) {
                try {
                    Map<String, Object> jdyData = transformService.convertToJdyFormat(order);
                    if (jdyData != null) {
                        dataList.add(jdyData);
                        validOrders.add(order);
                    } else {
                        LogUtil.logError("转换DM订单失败，跳过 (order_id=" + order.getId() + ")");
                        localDao.incrementSyncAttempts(order.getId());
                        localDao.updateSyncError(order.getId(), "数据转换失败");
                    }
                } catch (Exception e) {
                    LogUtil.logError("转换DM订单异常 (order_id=" + order.getId() + "): " + e.getMessage());
                    localDao.incrementSyncAttempts(order.getId());
                    localDao.updateSyncError(order.getId(), "数据转换异常: " + e.getMessage());
                }
            }
            
            if (dataList.isEmpty()) {
                continue;
            }
            
            // 批量创建
            int retryCount = 0;
            boolean batchSuccess = false;
            
            while (retryCount < MAX_RETRY && !batchSuccess) {
                try {
                    batchSuccess = apiService.createData(APP_ID, ENTRY_ID, dataList, false);
                    
                    if (batchSuccess) {
                        // 批量创建成功，更新所有订单的同步状态
                        for (org.example.dm.model.DmOrder order : validOrders) {
                            localDao.updateSyncStatus(order.getId(), 1);
                            successCount++;
                        }
                        
                        LogUtil.logInfo("批量创建DM订单成功: " + validOrders.size() + " 条");
                    } else {
                        retryCount++;
                        if (retryCount < MAX_RETRY) {
                            LogUtil.logWarning("批量创建DM订单失败，准备第 " + retryCount + " 次重试...");
                            Thread.sleep(RETRY_INTERVAL);
                        }
                    }
                } catch (Exception e) {
                    retryCount++;
                    LogUtil.logError("批量创建DM订单异常 (重试 " + retryCount + "/" + MAX_RETRY + "): " + e.getMessage());
                    
                    if (retryCount < MAX_RETRY) {
                        try {
                            Thread.sleep(RETRY_INTERVAL);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
            
            // 如果批量创建失败，更新所有订单的重试次数和错误信息
            if (!batchSuccess) {
                for (org.example.dm.model.DmOrder order : validOrders) {
                    localDao.incrementSyncAttempts(order.getId());
                    localDao.updateSyncError(order.getId(), "批量创建失败，已重试" + MAX_RETRY + "次");
                }
            }
        }
        
        return successCount;
    }
    
    /**
     * 逐个更新订单
     */
    private int updateOrders(List<org.example.dm.model.DmOrder> orders) {
        if (orders.isEmpty()) {
            return 0;
        }
        
        int successCount = 0;
        DmDataTransformService transformService = DmDataTransformService.getInstance();
        
        for (org.example.dm.model.DmOrder order : orders) {
            try {
                // 转换数据
                Map<String, Object> jdyData = transformService.convertToJdyFormat(order);
                if (jdyData == null) {
                    LogUtil.logError("转换DM订单失败，跳过 (order_id=" + order.getId() + ")");
                    localDao.incrementSyncAttempts(order.getId());
                    localDao.updateSyncError(order.getId(), "数据转换失败");
                    continue;
                }
                
                // 更新操作：先查询简道云获取data_id
                String jdyDataId = null;
                
                try {
                    // 通过order_no查询简道云
                    String orderNoWidgetId = configManager.getProperty("dm.jdy.orderNoWidget", "_widget_1770078767290");
                    Map<String, String> queryResult = apiService.queryData(APP_ID, ENTRY_ID, orderNoWidgetId, order.getOrderNo());
                    
                    if (queryResult.containsKey("data_id")) {
                        jdyDataId = queryResult.get("data_id");
                        LogUtil.logInfo("从简道云查询到data_id: " + jdyDataId + " (order_no=" + order.getOrderNo() + ")");
                    } else {
                        // 查询不到，可能是新记录，改为创建操作
                        LogUtil.logWarning("简道云中未找到该订单，改为创建操作 (order_no=" + order.getOrderNo() + ")");
                        
                        List<Map<String, Object>> dataList = new ArrayList<>();
                        dataList.add(jdyData);
                        boolean createSuccess = apiService.createData(APP_ID, ENTRY_ID, dataList, false);
                        
                        if (createSuccess) {
                            localDao.updateSyncStatus(order.getId(), 1);
                            successCount++;
                            LogUtil.logInfo("创建DM订单成功 (order_id=" + order.getId() + ")");
                        } else {
                            localDao.incrementSyncAttempts(order.getId());
                            localDao.updateSyncError(order.getId(), "创建失败");
                        }
                        
                        continue;
                    }
                } catch (Exception e) {
                    LogUtil.logError("查询简道云data_id失败 (order_id=" + order.getId() + "): " + e.getMessage());
                    localDao.incrementSyncAttempts(order.getId());
                    localDao.updateSyncError(order.getId(), "查询data_id失败: " + e.getMessage());
                    continue;
                }
                
                // 执行更新操作
                int retryCount = 0;
                boolean updateSuccess = false;
                
                while (retryCount < MAX_RETRY && !updateSuccess) {
                    try {
                        updateSuccess = apiService.updateData(APP_ID, ENTRY_ID, jdyDataId, jdyData);
                        
                        if (updateSuccess) {
                            localDao.updateSyncStatus(order.getId(), 1);
                            successCount++;
                            LogUtil.logInfo("更新DM订单成功 (order_id=" + order.getId() + ", jdy_data_id=" + jdyDataId + ")");
                        } else {
                            retryCount++;
                            if (retryCount < MAX_RETRY) {
                                LogUtil.logWarning("更新DM订单失败，准备第 " + retryCount + " 次重试 (order_id=" + order.getId() + ")");
                                Thread.sleep(RETRY_INTERVAL);
                            }
                        }
                    } catch (Exception e) {
                        retryCount++;
                        LogUtil.logError("更新DM订单异常 (order_id=" + order.getId() + ", 重试 " + retryCount + "/" + MAX_RETRY + "): " + e.getMessage());
                        
                        if (retryCount < MAX_RETRY) {
                            try {
                                Thread.sleep(RETRY_INTERVAL);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }
                
                // 如果更新失败，更新重试次数和错误信息
                if (!updateSuccess) {
                    localDao.incrementSyncAttempts(order.getId());
                    localDao.updateSyncError(order.getId(), "更新失败，已重试" + MAX_RETRY + "次");
                }
                
            } catch (Exception e) {
                LogUtil.logError("处理DM订单更新异常 (order_id=" + order.getId() + "): " + e.getMessage());
                localDao.incrementSyncAttempts(order.getId());
                localDao.updateSyncError(order.getId(), "处理异常: " + e.getMessage());
            }
        }
        
        return successCount;
    }
}
