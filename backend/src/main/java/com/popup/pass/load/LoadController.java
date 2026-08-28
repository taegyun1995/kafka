package com.popup.pass.load;

import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/**
 * 동시 요청을 밀어 넣는 버튼을 브라우저에 연다.
 *
 * <p>브라우저는 한 출처에 커넥션을 여섯 개까지만 연다. 그래서 "동시 300건"은 화면에서 만들 수 없고
 * 서버 안에서 만들어야 한다. {@link com.popup.pass.study.StudyController}와 같은 이유다.
 */
@RestController
@RequestMapping("/load")
@Profile("study")
public class LoadController {

    /** 눌러 볼 만한 지점들. 커넥션 풀(기본 10)을 사이에 두고 앞뒤를 고르게 잡았다. */
    private static final List<Integer> PRESETS = List.of(1, 5, 10, 30, 100, 300, 600);

    private final LoadRunner runner;
    private final LoadComparer comparer;

    public LoadController(LoadRunner runner, LoadComparer comparer) {
        this.runner = runner;
        this.comparer = comparer;
    }

    @GetMapping
    public Meta meta() {
        return new Meta(runner.maxPoolSize(), LoadRunner.MAX_CONCURRENCY,
                LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS, PRESETS);
    }

    @PostMapping("/runs")
    public LoadRunResult run(@RequestBody Request request) {
        int patience = request.clientPatienceMillis() == null
                ? LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS
                : request.clientPatienceMillis();
        try {
            return runner.run(request.concurrency(), patience);
        } catch (IllegalArgumentException rejected) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, rejected.getMessage());
        }
    }

    /**
     * 같은 부하를 보호 없는 코드와 다 켠 코드에 한 번씩 건다.
     * 두 번 실행하므로 응답이 그만큼 늦다.
     */
    @PostMapping("/comparisons")
    public LoadComparison compare(@RequestBody Request request) {
        int patience = request.clientPatienceMillis() == null
                ? LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS
                : request.clientPatienceMillis();
        try {
            return comparer.compare(request.concurrency(), patience);
        } catch (IllegalArgumentException rejected) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, rejected.getMessage());
        }
    }

    public record Request(int concurrency, Integer clientPatienceMillis) {
    }

    public record Meta(int maxPoolSize, int maxConcurrency, int defaultClientPatienceMillis, List<Integer> presets) {
    }
}
