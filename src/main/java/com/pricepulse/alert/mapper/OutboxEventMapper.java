package com.pricepulse.alert.mapper;

import com.pricepulse.alert.model.OutboxEvent;
import com.pricepulse.alert.model.OutboxStatus;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface OutboxEventMapper {

    @Insert("""
            INSERT INTO outbox_events (aggregate_id, event_type, payload, status, attempts, next_retry_at, created_at)
            VALUES (#{aggregateId}, #{eventType}, #{payload}, #{status}, #{attempts}, NOW(), NOW())
            """)
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(OutboxEvent event);

    @Select("""
            SELECT id, aggregate_id, event_type, payload, status, locked_by, locked_until,
                   attempts, next_retry_at, created_at, published_at
            FROM outbox_events
            WHERE status = 'PENDING'
              AND (locked_until IS NULL OR locked_until < NOW())
              AND next_retry_at <= NOW()
            ORDER BY id
            LIMIT #{batchSize}
            FOR UPDATE SKIP LOCKED
            """)
    List<OutboxEvent> selectPendingForUpdateSkipLocked(@Param("batchSize") int batchSize);

    @Update("""
            <script>
            UPDATE outbox_events
            SET locked_by = #{leaseId},
                locked_until = DATE_ADD(NOW(), INTERVAL #{leaseSeconds} SECOND)
            WHERE id IN
            <foreach collection="ids" item="id" open="(" separator="," close=")">
                #{id}
            </foreach>
              AND status = 'PENDING'
            </script>
            """)
    int lockEvents(@Param("ids") List<Long> ids,
                   @Param("leaseId") String leaseId,
                   @Param("leaseSeconds") int leaseSeconds);

    @Update("""
            UPDATE outbox_events
            SET status = 'SENT',
                published_at = NOW(),
                locked_by = NULL,
                locked_until = NULL
            WHERE id = #{id}
              AND status = 'PENDING'
              AND locked_by = #{leaseId}
            """)
    int markSent(@Param("id") Long id, @Param("leaseId") String leaseId);

    @Update("""
            UPDATE outbox_events
            SET attempts = attempts + 1,
                next_retry_at = DATE_ADD(NOW(), INTERVAL #{backoffSeconds} SECOND),
                locked_by = NULL,
                locked_until = NULL
            WHERE id = #{id}
              AND status = 'PENDING'
              AND locked_by = #{leaseId}
            """)
    int markRetry(@Param("id") Long id,
                  @Param("leaseId") String leaseId,
                  @Param("backoffSeconds") long backoffSeconds);

    @Update("""
            UPDATE outbox_events
            SET next_retry_at = DATE_ADD(NOW(), INTERVAL #{delaySeconds} SECOND),
                locked_by = NULL,
                locked_until = NULL
            WHERE id = #{id}
              AND status = 'PENDING'
              AND locked_by = #{leaseId}
            """)
    int releaseLeaseWithDelay(@Param("id") Long id,
                              @Param("leaseId") String leaseId,
                              @Param("delaySeconds") long delaySeconds);

    @Select("""
            SELECT id, aggregate_id, event_type, payload, status, locked_by, locked_until,
                   attempts, next_retry_at, created_at, published_at
            FROM outbox_events
            WHERE id = #{id}
            """)
    OutboxEvent findById(@Param("id") Long id);

    @Select("""
            SELECT id, aggregate_id, event_type, payload, status, locked_by, locked_until,
                   attempts, next_retry_at, created_at, published_at
            FROM outbox_events
            WHERE aggregate_id = #{aggregateId}
            """)
    OutboxEvent findByAggregateId(@Param("aggregateId") Long aggregateId);

    @Select("SELECT COUNT(*) FROM outbox_events WHERE status = #{status}")
    long countByStatus(@Param("status") OutboxStatus status);
}
