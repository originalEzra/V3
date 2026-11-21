package com.pricepulse.alert.mapper;

import com.pricepulse.alert.model.Notification;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface NotificationMapper {

    @Insert("""
            INSERT INTO notifications (trigger_id, user_id, stock_id, content, is_read, created_at)
            VALUES (#{triggerId}, #{userId}, #{stockId}, #{content}, #{isRead}, #{createdAt})
            """)
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(Notification notification);

    @Select("""
            SELECT id, trigger_id, user_id, stock_id, content, is_read, created_at
            FROM notifications
            WHERE user_id = #{userId}
            ORDER BY id DESC
            LIMIT #{limit}
            """)
    List<Notification> findByUserId(@Param("userId") Long userId, @Param("limit") int limit);

    @Select("""
            SELECT id, trigger_id, user_id, stock_id, content, is_read, created_at
            FROM notifications
            ORDER BY id DESC
            LIMIT #{limit}
            """)
    List<Notification> findRecentAll(@Param("limit") int limit);

    @Select("""
            SELECT id, trigger_id, user_id, stock_id, content, is_read, created_at
            FROM notifications
            WHERE trigger_id = #{triggerId}
            """)
    Notification findByTriggerId(@Param("triggerId") Long triggerId);

    @Select("SELECT COUNT(*) FROM notifications WHERE user_id = #{userId} AND is_read = 0")
    long countUnreadByUserId(@Param("userId") Long userId);

    @Select("SELECT COUNT(*) FROM notifications")
    long countAll();

    @Update("UPDATE notifications SET is_read = 1 WHERE id = #{id} AND is_read = 0")
    int markReadById(@Param("id") Long id);

    @Update("UPDATE notifications SET is_read = 1 WHERE user_id = #{userId} AND is_read = 0")
    int markAllReadByUserId(@Param("userId") Long userId);

    @Update("UPDATE notifications SET is_read = 1 WHERE is_read = 0")
    int markAllRead();
}
