package org.example.dm.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dm.config.DmConfigManager;
import org.example.dm.dao.DmLocalDao;
import org.example.dm.model.DmOrder;
import org.example.dm.model.DmOrderDetail;
import org.example.util.LogUtil;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * DM数据转换服务
 * 负责将DM订单数据转换为简道云API格式
 */
public class DmDataTransformService {
    private static DmDataTransformService instance;
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final ObjectMapper mapper = new ObjectMapper();
    
    // 字段映射配置
    private Map<String, String> mainFieldMapping;
    private Map<String, String> subTableMapping;
    private String subTableWidgetId;
    
    private DmDataTransformService() {
        loadFieldMapping();
    }
    
    public static synchronized DmDataTransformService getInstance() {
        if (instance == null) {
            instance = new DmDataTransformService();
        }
        return instance;
    }
    
    /**
     * 加载字段映射配置
     */
    private void loadFieldMapping() {
        try {
            DmConfigManager configManager = DmConfigManager.getInstance();
            String mappingFile = configManager.getProperty("dm.jdy.fieldMapping", "dm_to_jdy_field_mapping.json");
            
            InputStream input = getClass().getClassLoader().getResourceAsStream(mappingFile);
            if (input == null) {
                // 尝试从工作目录加载
                input = new java.io.FileInputStream(mappingFile);
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> config = mapper.readValue(input, Map.class);
            
            // 加载主表字段映射
            @SuppressWarnings("unchecked")
            Map<String, String> mainFields = (Map<String, String>) config.get("main_fields");
            this.mainFieldMapping = mainFields;
            
            // 加载子表映射
            @SuppressWarnings("unchecked")
            Map<String, Object> subTables = (Map<String, Object>) config.get("sub_tables");
            if (subTables != null && subTables.containsKey("order_details")) {
                @SuppressWarnings("unchecked")
                Map<String, String> detailsMapping = (Map<String, String>) subTables.get("order_details");
                this.subTableMapping = detailsMapping;
                
                // 获取子表widget ID
                this.subTableWidgetId = mainFields.get("order_details");
            }
            
            LogUtil.logInfo("DM字段映射配置加载成功");
            
        } catch (Exception e) {
            LogUtil.logError("加载DM字段映射配置失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("加载DM字段映射配置失败", e);
        }
    }
    
    /**
     * 将DM订单转换为简道云格式
     * @param order DM订单对象
     * @return 简道云格式的数据
     */
    public Map<String, Object> convertToJdyFormat(DmOrder order) {
        Map<String, Object> converted = new HashMap<>();
        
        try {
            // 转换主表字段
            convertMainFields(order, converted);
            
            // 转换子表数据
            convertSubTableData(order.getId(), converted);
            
        } catch (Exception e) {
            LogUtil.logError("转换DM订单数据失败 (order_id=" + order.getId() + "): " + e.getMessage());
            e.printStackTrace();
            return null;
        }
        
        return converted;
    }
    
    /**
     * 转换主表字段
     */
    private void convertMainFields(DmOrder order, Map<String, Object> converted) {
        // order_no - 工单号
        putField(converted, "order_no", order.getOrderNo());
        
        // month_settlement - 月结
        putField(converted, "month_settlement", order.getMonthSettlement());
        
        // factory - 工厂
        putField(converted, "factory", order.getFactory());
        
        // person_in_charge - 负责人
        putField(converted, "person_in_charge", order.getPersonInCharge());
        
        // currency - 币种
        putField(converted, "currency", order.getCurrency());
        
        // mark - 标记
        putField(converted, "mark", order.getMark());
        
        // tax_rate - 税率
        putField(converted, "tax_rate", order.getTaxRate() != null ? order.getTaxRate().toString() : "");
        
        // payment_terms - 付款条件
        putField(converted, "payment_terms", order.getPaymentTerms());
        
        // remarks - 备注
        putField(converted, "remarks", order.getRemarks());
        
        // in_warehouse - 入库仓
        putField(converted, "in_warehouse", order.getInWarehouse());
        
        // material_warehouse - 物料仓
        putField(converted, "material_warehouse", order.getMaterialWarehouse());
        
        // original_terms - 原始条款
        putField(converted, "original_terms", order.getOriginalTerms());
        
        // total_quantity - 总数量
        putField(converted, "total_quantity", order.getTotalQuantity() != null ? order.getTotalQuantity().toString() : "");
        
        // total_tax_amount - 总含税金额
        putField(converted, "total_tax_amount", order.getTotalTaxAmount() != null ? order.getTotalTaxAmount().toString() : "");
        
        // department - 部门
        putField(converted, "department", order.getDepartment());
        
        // creator - 创建人
        putField(converted, "creator", order.getCreator());
        
        // auditor - 审核人
        putField(converted, "auditor", order.getAuditor());
        
        // approver - 批准人
        putField(converted, "approver", order.getApprover());
        
        // submit_time - 提交时间
        if (order.getSubmitTime() != null) {
            putField(converted, "submit_time", order.getSubmitTime().format(DATETIME_FORMATTER));
        } else {
            putField(converted, "submit_time", "");
        }
        
        // modify_time - 修改时间
        putField(converted, "modify_time", order.getModifyTime().format(DATETIME_FORMATTER));
        
        // order_status - 工单状态
        putField(converted, "order_status", String.valueOf(order.getOrderStatus()));
    }
    
    /**
     * 转换子表数据
     */
    private void convertSubTableData(Integer orderId, Map<String, Object> converted) {
        DmLocalDao localDao = DmLocalDao.getInstance();
        List<DmOrderDetail> details = localDao.queryOrderDetails(orderId);
        
        if (details == null || details.isEmpty()) {
            // 子表为空，设置空数组
            converted.put(subTableWidgetId, Collections.singletonMap("value", new ArrayList<>()));
            return;
        }
        
        List<Map<String, Object>> subTableData = new ArrayList<>();
        
        for (DmOrderDetail detail : details) {
            Map<String, Object> detailMap = new HashMap<>();
            
            // line_no - 行号
            putSubField(detailMap, "line_no", detail.getLineNo() != null ? String.valueOf(detail.getLineNo()) : "");
            
            // material_code - 物料编码
            putSubField(detailMap, "material_code", detail.getMaterialCode());
            
            // material_desc - 物料描述
            putSubField(detailMap, "material_desc", detail.getMaterialDesc());
            
            // quantity - 数量
            putSubField(detailMap, "quantity", detail.getQuantity() != null ? detail.getQuantity().toString() : "");
            
            // unit_price - 单价
            putSubField(detailMap, "unit_price", detail.getUnitPrice() != null ? detail.getUnitPrice().toString() : "");
            
            // tax_unit_price - 含税单价
            putSubField(detailMap, "tax_unit_price", detail.getTaxUnitPrice() != null ? detail.getTaxUnitPrice().toString() : "");
            
            // tax_amount - 含税金额
            putSubField(detailMap, "tax_amount", detail.getTaxAmount() != null ? detail.getTaxAmount().toString() : "");
            
            // price_book - 价格手册
            putSubField(detailMap, "price_book", detail.getPriceBook());
            
            // suggested_quantity - 建议数量
            putSubField(detailMap, "suggested_quantity", detail.getSuggestedQuantity() != null ? detail.getSuggestedQuantity().toString() : "");
            
            // source_doc_no - 源单号
            putSubField(detailMap, "source_doc_no", detail.getSourceDocNo());
            
            subTableData.add(detailMap);
        }
        
        // 按照简道云格式包装子表数据
        converted.put(subTableWidgetId, Collections.singletonMap("value", subTableData));
    }
    
    /**
     * 添加主表字段（包装为简道云格式）
     */
    private void putField(Map<String, Object> converted, String fieldName, Object value) {
        String widgetId = mainFieldMapping.get(fieldName);
        if (widgetId != null && !widgetId.equals(subTableWidgetId)) {
            String strValue = (value != null) ? value.toString().trim() : "";
            converted.put(widgetId, Collections.singletonMap("value", strValue));
        }
    }
    
    /**
     * 添加子表字段（包装为简道云格式）
     */
    private void putSubField(Map<String, Object> detailMap, String fieldName, Object value) {
        String widgetId = subTableMapping.get(fieldName);
        if (widgetId != null) {
            String strValue = (value != null) ? value.toString().trim() : "";
            detailMap.put(widgetId, Collections.singletonMap("value", strValue));
        }
    }
}
