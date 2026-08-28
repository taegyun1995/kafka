package com.popup.pass.study;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 학습용 전용 테이블을 매 실행마다 새로 만든다.
 * 실제 예약 테이블과 이름이 겹치지 않으므로 서로 영향을 주지 않는다.
 */
public class StudyTables {

    public static final String ROUND_ID = "study-round";

    private final JdbcTemplate jdbc;

    public StudyTables(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** 부분 Unique Index는 2단계부터 건다. 단계가 무엇을 갖고 시작하는지가 여기서 갈린다. */
    public void reset(boolean withConfirmedUniqueIndex, int capacity) {
        jdbc.execute("drop table if exists study_reservations");
        jdbc.execute("drop table if exists study_rounds");
        jdbc.execute("""
                create table study_rounds (
                    id              varchar(80) primary key,
                    capacity        int not null,
                    remaining_count int not null
                )
                """);
        jdbc.execute("""
                create table study_reservations (
                    request_id varchar(36) primary key,
                    round_id   varchar(80) not null,
                    user_id    varchar(80) not null,
                    status     varchar(32) not null
                )
                """);
        if (withConfirmedUniqueIndex) {
            jdbc.execute("""
                    create unique index uk_study_confirmed on study_reservations (round_id, user_id)
                        where status = 'CONFIRMED'
                    """);
        }
        jdbc.update("insert into study_rounds (id, capacity, remaining_count) values (?, ?, ?)",
                ROUND_ID, capacity, capacity);
    }

    public int remainingCount() {
        return jdbc.queryForObject(
                "select remaining_count from study_rounds where id = ?", Integer.class, ROUND_ID);
    }

    public int rowCount() {
        return jdbc.queryForObject("select count(*) from study_reservations", Integer.class);
    }
}
