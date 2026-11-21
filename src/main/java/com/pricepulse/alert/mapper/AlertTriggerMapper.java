package com.pricepulse.alert.mapper;

import com.pricepulse.alert.model.AlertTrigger;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface AlertTriggerMapper {

    @Insert("""
            INSERT INTO alert_triggers (rule_id, user_id, stock_id, direction, threshold_price, trigger_price, triggered_at)
            VALUES (#{ruleId}, #{userId}, #{stockId}, #{direction}, #{thresholdPrice}, #{triggerPrice}, #{triggeredAt})
            """)
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(AlertTrigger trigger);

    @Select("""
            SELECT id, rule_id, user_id, stock_id, direction, threshold_price, trigger_price, triggered_at
            FROM alert_triggers
            WHERE id = #{id}
            """)
    AlertTrigger findById(@Param("id") Long id);

    @Select("""
            SELECT id, rule_id, user_id, stock_id, direction, threshold_price, trigger_price, triggered_at
            FROM alert_triggers
            WHERE rule_id = #{ruleId}
            """)
    AlertTrigger findByRuleId(@Param("ruleId") Long ruleId);

    @Select("SELECT COUNT(*) FROM alert_triggers")
    long countAll();
}
