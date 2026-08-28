package com.popup.pass.support;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * 실제 PostgreSQL에서 도는 테스트의 공통 바탕이다.
 *
 * <p>Container를 직접 start하고 stop하지 않는다. {@code @Testcontainers}는 테스트 클래스가 끝날 때마다
 * Container를 멈추므로, 여러 클래스가 같은 Spring Context를 나눠 쓰면 먼저 끝난 클래스가 DB를 내려버린다.
 * 여기서는 JVM에 하나만 띄우고 종료 시 Ryuk이 정리하게 둔다.
 */
@SpringBootTest
public abstract class PostgresBackedTest {

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    static {
        POSTGRES.start();
    }
}
