package com.pricepulse.market.mapper;

import com.pricepulse.market.model.KlineBar;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface KlineMapper {

    @Insert("""
            <script>
            INSERT INTO kline_1m (stock_id, minute_ts, open_price, high_price, low_price, close_price)
            VALUES
            <foreach collection="bars" item="b" separator=",">
                (#{b.stockId}, #{b.minuteTs}, #{b.openPrice}, #{b.highPrice}, #{b.lowPrice}, #{b.closePrice})
            </foreach>
            ON DUPLICATE KEY UPDATE
                high_price = GREATEST(high_price, VALUES(high_price)),
                low_price = LEAST(low_price, VALUES(low_price)),
                close_price = VALUES(close_price)
            </script>
            """)
    int batchUpsert(@Param("bars") List<KlineBar> bars);

    @Select("""
            SELECT stock_id, minute_ts, open_price, high_price, low_price, close_price
            FROM kline_1m
            WHERE stock_id = #{stockId}
            ORDER BY minute_ts DESC
            LIMIT #{limit}
            """)
    List<KlineBar> findRecentByStockId(@Param("stockId") Long stockId, @Param("limit") int limit);
}
