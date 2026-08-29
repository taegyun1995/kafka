-- 모든 시도(확정/거절)를 reservations에 남기고, 업무 시도 키는 DB가 유일하게 보장한다.
-- 확정 예약의 유일성은 애플리케이션의 조회 한 줄에만 기대고 있다.
alter table if exists reservations drop constraint if exists uk_reservation_round_user;
drop index if exists uk_reservation_confirmed_round_user;

-- 응답 유실 뒤 같은 업무 요청을 다시 찾을 수 있도록 키를 모든 시도에 붙인다.
-- 기존 행은 서버가 만들었던 request_id를 임시 키로 채운 뒤 새 요청부터 클라이언트 키를 사용한다.
alter table if exists reservations add column if not exists idempotency_key varchar(128);
update reservations set idempotency_key = request_id where idempotency_key is null;
alter table if exists reservations alter column idempotency_key set not null;
create unique index if not exists uk_reservation_idempotency_key on reservations (idempotency_key);

-- 예약 상태 목록은 애플리케이션 enum과 같아야 한다.
-- Hibernate는 테이블을 처음 만들 때만 check 제약을 만들고, ddl-auto=update는 이미 있는 제약을 고치지 않는다.
alter table if exists reservations drop constraint if exists reservations_status_check;
alter table if exists reservations add constraint reservations_status_check
    check (status in ('CONFIRMED', 'REJECTED_SOLD_OUT', 'REJECTED_DUPLICATE', 'REJECTED_ROUND_CLOSED', 'REJECTED_ROUND_NOT_FOUND'));
