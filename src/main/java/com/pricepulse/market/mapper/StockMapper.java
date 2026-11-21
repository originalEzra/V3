package com.pricepulse.market.mapper;

import com.pricepulse.market.model.Stock;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface StockMapper {

    @Select("SELECT id, symbol, name, status FROM stocks WHERE status = 'ACTIVE' ORDER BY id")
    List<Stock> findAllActive();

    @Select("SELECT id, symbol, name, status FROM stocks WHERE symbol = #{symbol}")
    Stock findBySymbol(@Param("symbol") String symbol);

    @Select("SELECT id, symbol, name, status FROM stocks WHERE id = #{id}")
    Stock findById(@Param("id") Long id);
}
