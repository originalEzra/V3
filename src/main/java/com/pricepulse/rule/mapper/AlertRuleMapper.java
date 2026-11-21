package com.pricepulse.rule.mapper;

import com.pricepulse.rule.model.AlertRule;
import com.pricepulse.rule.model.Direction;
import java.math.BigDecimal;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface AlertRuleMapper {

    @Insert("""
            INSERT INTO alert_rules (user_id, stock_id, direction, threshold_price, status, version)
            VALUES (#{userId}, #{stockId}, #{direction}, #{thresholdPrice}, #{status}, #{version})
            """)
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(AlertRule rule);

    @Select("SELECT id, user_id, stock_id, direction, threshold_price, status, version, created_at, updated_at " +
            "FROM alert_rules WHERE id = #{id}")
    AlertRule findById(@Param("id") Long id);

    @Select("SELECT id, user_id, stock_id, direction, threshold_price, status, version, created_at, updated_at " +
            "FROM alert_rules WHERE status = 'ACTIVE' ORDER BY id")
    List<AlertRule> findActiveAll();

    @Select("SELECT id, user_id, stock_id, direction, threshold_price, status, version, created_at, updated_at " +
            "FROM alert_rules WHERE user_id = #{userId} AND status != 'DELETED' ORDER BY id DESC")
    List<AlertRule> findByUserId(@Param("userId") Long userId);

    @Select("SELECT id, user_id, stock_id, direction, threshold_price, status, version, created_at, updated_at " +
            "FROM alert_rules WHERE stock_id = #{stockId} AND status != 'DELETED' ORDER BY id DESC")
    List<AlertRule> findByStockId(@Param("stockId") Long stockId);

    @Select("SELECT id, user_id, stock_id, direction, threshold_price, status, version, created_at, updated_at " +
            "FROM alert_rules WHERE user_id = #{userId} AND stock_id = #{stockId} AND status != 'DELETED' ORDER BY id DESC")
    List<AlertRule> findByUserIdAndStockId(@Param("userId") Long userId, @Param("stockId") Long stockId);

    @Update("""
            UPDATE alert_rules
            SET direction = #{direction},
                threshold_price = #{thresholdPrice},
                version = version + 1,
                updated_at = NOW()
            WHERE id = #{id}
              AND status = 'ACTIVE'
              AND version = #{version}
            """)
    int updateRuleOptimistic(@Param("id") Long id,
                             @Param("direction") Direction direction,
                             @Param("thresholdPrice") BigDecimal thresholdPrice,
                             @Param("version") Long version);

    @Update("""
            UPDATE alert_rules
            SET status = 'DELETED',
                version = version + 1,
                updated_at = NOW()
            WHERE id = #{id}
              AND status = 'ACTIVE'
              AND version = #{version}
            """)
    int softDeleteOptimistic(@Param("id") Long id, @Param("version") Long version);

    @Select("""
            <script>
            SELECT id, user_id, stock_id, direction, threshold_price, status, version, created_at, updated_at
            FROM alert_rules
            WHERE id IN
            <foreach collection="ids" item="id" open="(" separator="," close=")">
                #{id}
            </foreach>
            </script>
            """)
    List<AlertRule> findByIds(@Param("ids") List<Long> ids);

    @Update("""
            UPDATE alert_rules
            SET status = 'COMPLETED',
                version = version + 1,
                updated_at = NOW()
            WHERE id = #{id}
              AND stock_id = #{stockId}
              AND status = 'ACTIVE'
              AND version = #{version}
              AND ((direction = 'ABOVE' AND threshold_price <= #{price})
                   OR (direction = 'BELOW' AND threshold_price >= #{price}))
            """)
    int completeRuleOptimistic(@Param("id") Long id,
                               @Param("stockId") Long stockId,
                               @Param("version") Long version,
                               @Param("price") BigDecimal price);

    @Select("""
            SELECT id FROM alert_rules
            WHERE stock_id = #{stockId} AND status = 'ACTIVE' AND direction = 'ABOVE' AND threshold_price <= #{price}
            UNION ALL
            SELECT id FROM alert_rules
            WHERE stock_id = #{stockId} AND status = 'ACTIVE' AND direction = 'BELOW' AND threshold_price >= #{price}
            """)
    List<Long> findMatchingRuleIdsFallback(@Param("stockId") Long stockId, @Param("price") BigDecimal price);
}
