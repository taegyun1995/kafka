-- 모든 시도(확정/거절)를 reservations에 남긴다. 지금은 DB가 중복을 막지 않는다.
-- 확정 예약의 유일성은 애플리케이션의 조회 한 줄에만 기대고 있다.
alter table if exists reservations drop constraint if exists uk_reservation_round_user;
drop index if exists uk_reservation_confirmed_round_user;

-- 예약 상태 목록은 애플리케이션 enum과 같아야 한다.
-- Hibernate는 테이블을 처음 만들 때만 check 제약을 만들고, ddl-auto=update는 이미 있는 제약을 고치지 않는다.
alter table if exists reservations drop constraint if exists reservations_status_check;
alter table if exists reservations add constraint reservations_status_check
    check (status in ('CONFIRMED', 'REJECTED_SOLD_OUT', 'REJECTED_DUPLICATE', 'REJECTED_ROUND_CLOSED', 'REJECTED_ROUND_NOT_FOUND'));
