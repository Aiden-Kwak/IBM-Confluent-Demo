-- ================================================
-- ksqlDB CLI에서 직접 실행할 수 있는 쿼리 모음
-- ================================================
-- 접속: docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

-- ──────────────────────────────────────────────
-- 기본 조회
-- ──────────────────────────────────────────────

-- 현재 스트림/테이블 목록
SHOW STREAMS;
SHOW TABLES;
SHOW TOPICS;

-- ──────────────────────────────────────────────
-- Push Query (실시간 구독 - 새 데이터가 올 때마다 출력)
-- ──────────────────────────────────────────────

-- 모든 주문 실시간 모니터링
SELECT * FROM orders_stream EMIT CHANGES;

-- 전자제품 주문만 필터링
SELECT order_id, customer, product, total_amount
FROM orders_stream
WHERE category = 'electronics'
EMIT CHANGES;

-- 서울 지역 대량 주문 (3개 이상)
SELECT order_id, customer, product, quantity, total_amount
FROM orders_stream
WHERE region = '서울' AND quantity >= 3
EMIT CHANGES;

-- 고가 주문 실시간 알림
SELECT * FROM high_value_orders EMIT CHANGES;

-- 취소 주문 실시간 알림
SELECT * FROM cancelled_orders EMIT CHANGES;

-- ──────────────────────────────────────────────
-- Pull Query (현재 상태 1회 조회)
-- ──────────────────────────────────────────────

-- 지역별 주문 현황
SELECT * FROM orders_by_region;

-- ──────────────────────────────────────────────
-- 윈도우 집계 (시간 기반)
-- ──────────────────────────────────────────────

-- 30초 단위 Tumbling Window - 주문 수와 매출
SELECT
    category,
    COUNT(*) AS cnt,
    SUM(total_amount) AS sales,
    WINDOWSTART AS ws,
    WINDOWEND AS we
FROM orders_stream
WINDOW TUMBLING (SIZE 30 SECONDS)
GROUP BY category
EMIT CHANGES;

-- 1분 Hopping Window (30초 간격) - 이동 평균
SELECT
    region,
    COUNT(*) AS cnt,
    AVG(total_amount) AS avg_amount,
    WINDOWSTART AS ws,
    WINDOWEND AS we
FROM orders_stream
WINDOW HOPPING (SIZE 1 MINUTE, ADVANCE BY 30 SECONDS)
GROUP BY region
EMIT CHANGES;

-- ──────────────────────────────────────────────
-- 고급: 스트림 JOIN
-- ──────────────────────────────────────────────

-- 예시: CDC 주문 + 실시간 주문 합치기 (UNION 느낌)
-- 실제 환경에서는 주문 스트림 + 결제 스트림을 JOIN하여
-- 결제 완료된 주문만 필터링하는 등의 활용이 가능합니다.
