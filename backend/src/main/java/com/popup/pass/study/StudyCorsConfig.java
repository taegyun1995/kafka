package com.popup.pass.study;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * 정적 문서 사이트가 다른 포트에서 열리므로, 화면이 API를 부를 수 있게 CORS를 연다.
 *
 * <p>{@code study} 프로필에서만 적용된다. 평소 실행에는 이 설정이 없으므로 예약 API는 브라우저에서 열리지 않는다.
 * 로컬 학습용이라 출처를 가리지 않는다.
 */
@Configuration
@Profile("study")
public class StudyCorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOriginPatterns("*")
                .allowedMethods("GET", "POST");
    }
}
