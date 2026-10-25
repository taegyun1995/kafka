(function () {
  "use strict";

  const form = document.querySelector("[data-yaml-builder]");
  if (!form) return;
  const field = (id) => document.getElementById(id);
  const value = (id) => field(id).value.trim();
  const checked = (id) => field(id).checked;
  const scalar = (input) => JSON.stringify(String(input));
  const env = (name, input) => scalar(checked("yaml-use-env") ? "${" + name + ":" + input + "}" : input);
  const output = field("yaml-output");
  const validation = form.querySelector("[data-builder-validation]");
  const summary = form.querySelector("[data-builder-summary]");
  const exportStatus = form.querySelector("[data-builder-export-status]");
  const exports = Array.from(form.querySelectorAll("[data-copy-target], [data-builder-download]"));
  const presets = Array.from(document.querySelectorAll("[data-yaml-preset]"));
  const presetStatus = document.querySelector("[data-builder-status]");
  const scopeStatus = form.querySelector("[data-builder-scope-status]");
  const classes = {
    string: ["org.apache.kafka.common.serialization.StringSerializer", "org.apache.kafka.common.serialization.StringDeserializer"],
    bytes: ["org.apache.kafka.common.serialization.ByteArraySerializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"],
    json: ["org.springframework.kafka.support.serializer.JsonSerializer", "org.springframework.kafka.support.serializer.JsonDeserializer"]
  };
  let activePreset = "";
  let valid = false;

  // Descriptions below explain choices; validation and generation remain separate.
    const yamlValueDescriptions = {
      "yaml-producer-acks": {
        all: "권장 기본값입니다. Leader가 모든 ISR의 확인을 받을 때까지 성공으로 응답해 내구성을 높입니다.",
        "1": "Leader Broker의 로컬 기록만 확인합니다. 지연은 낮지만 Leader 장애와 복제 지연 상황의 내구성이 낮아질 수 있습니다.",
        "0": "Broker 응답을 기다리지 않아 확인에 드는 대기를 줄입니다. Broker가 Record를 받았는지 Producer가 확인할 수 없어 유실 위험이 큽니다."
      },
      "yaml-producer-compression": {
        none: "압축하지 않습니다. CPU 부담은 가장 낮지만 네트워크·디스크 사용량이 커집니다.",
        gzip: "압축률이 높지만 CPU 비용과 지연이 커질 수 있습니다.",
        lz4: "압축·CPU·지연의 균형이 좋아 일반적인 실시간 Producer의 시작점으로 무난합니다.",
        snappy: "낮은 지연과 적은 CPU 비용을 우선하는 선택입니다. 압축률은 데이터에 따라 달라집니다.",
        zstd: "높은 압축률을 기대할 수 있지만 Producer·Broker CPU 비용을 함께 측정해야 합니다."
      },
      "yaml-consumer-offset-reset": {
        earliest: "Commit Offset이 없으면 해당 Partition의 보존된 가장 이른 Record부터 읽습니다. 재처리·초기 적재에 적합합니다.",
        latest: "Commit Offset이 없으면 현재 끝 이후의 새 Record부터 읽습니다. 기존 데이터는 건너뛸 수 있습니다.",
        none: "Commit Offset이 없거나 유효하지 않으면 자동 위치를 정하지 않고 예외를 발생시킵니다. 운영 판단을 코드로 강제할 때 사용합니다."
      },
      "yaml-consumer-isolation": {
        read_uncommitted: "아직 완료되지 않았거나 Abort된 Transaction Record도 볼 수 있습니다. 일반 비트랜잭션 소비에 사용합니다.",
        read_committed: "Commit된 Transaction Record와 비트랜잭션 Record를 읽습니다. 열린 Transaction 뒤의 Record는 LSO 경계 때문에 함께 대기할 수 있으며, Consumer 처리 자체를 Exactly-once로 만들지는 않습니다."
      },
      "yaml-consumer-assignment": {
        "org.apache.kafka.clients.consumer.CooperativeStickyAssignor": "기존 할당을 최대한 유지하면서 점진적으로 이동합니다. Rebalance 중 중단 범위를 줄이기 위한 권장 시작점입니다.",
        "org.apache.kafka.clients.consumer.RangeAssignor": "Topic별 Partition 범위를 나눕니다. 여러 Topic을 함께 구독하면 분배가 치우칠 수 있습니다.",
        "org.apache.kafka.clients.consumer.RoundRobinAssignor": "모든 Topic·Partition을 순환 배분해 균형을 노립니다. 구성 변화 때 할당 이동이 커질 수 있습니다.",
        "org.apache.kafka.clients.consumer.StickyAssignor": "가능한 기존 할당을 유지하며 균형을 맞춥니다. Cooperative 방식이 아닌 전체 Rebalance 계열입니다."
      },
      "yaml-listener-ack-mode": {
        record: "각 Record 처리가 끝난 뒤 해당 위치를 Commit하는 기본적인 단위입니다. 실패 시 처리 완료된 Record 이후부터 재개하기 쉽습니다.",
        batch: "한 번의 poll()에서 받은 Batch 처리가 끝난 뒤 Commit합니다. Batch 내 하나가 실패하면 더 넓은 범위가 재처리될 수 있습니다.",
        manual: "Listener 코드에서 Acknowledgment.acknowledge()를 호출한 뒤, poll() Batch 처리 경계에서 Commit합니다. Ack 호출 즉시 Commit하는 방식은 아닙니다.",
        manual_immediate: "Listener가 Consumer Thread에서 Ack를 호출하는 즉시 Commit을 시도합니다. 호출 위치와 Thread에 따라 동작 차이가 있습니다."
      }
    };

    function updateYamlValueDescriptions() {
      form.querySelectorAll("[data-yaml-value-help]").forEach(function (help) {
        const id = help.dataset.yamlValueHelp;
        const input = field(id);
        const descriptions = yamlValueDescriptions[id];
        help.textContent = input && descriptions ? (descriptions[input.value] || "") : "";
      });

      form.querySelectorAll("[data-yaml-choice-guide]").forEach(function (guide) {
        const id = guide.dataset.yamlChoiceGuide;
        const input = field(id);
        const descriptions = yamlValueDescriptions[id];
        guide.replaceChildren();
        if (!input || !descriptions) return;

        const list = document.createElement("ul");
        Object.entries(descriptions).forEach(function (entry) {
          const optionValue = entry[0];
          const description = entry[1];
          const item = document.createElement("li");
          const label = document.createElement("strong");
          const detail = document.createElement("span");
          const shortValue = optionValue.indexOf(".") >= 0
            ? optionValue.slice(optionValue.lastIndexOf(".") + 1)
            : optionValue;

          label.textContent = shortValue;
          detail.textContent = description;
          item.classList.toggle("is-selected", optionValue === input.value);
          item.append(label, detail);
          list.appendChild(item);
        });
        guide.appendChild(list);
      });
    }

  function markPreset(name) {
    activePreset = name;
    presets.forEach((button) => {
      const active = button.dataset.yamlPreset === name;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", String(active));
    });
    const button = presets.find((item) => item.dataset.yamlPreset === name);
    presetStatus.textContent = button
      ? "현재 프리셋: " + button.textContent + " · 실제 보장은 처리·Commit 코드까지 함께 결정합니다."
      : "직접 옵션을 조정할 수 있습니다. 설정과 처리 코드의 보장을 함께 확인하세요.";
  }

  function showSummary(messages) {
    summary.replaceChildren();
    messages.forEach((message) => {
      const item = document.createElement("li");
      item.textContent = message;
      summary.appendChild(item);
    });
  }

  function update() {
    const producer = checked("yaml-include-producer");
    const listener = checked("yaml-include-listener");
    const consumerToggle = field("yaml-include-consumer");
    consumerToggle.disabled = listener;
    if (listener) consumerToggle.checked = true;
    const consumer = consumerToggle.checked;
    const transaction = producer && checked("yaml-producer-transaction");
    const idempotenceToggle = field("yaml-producer-idempotence");
    idempotenceToggle.disabled = transaction;
    if (transaction) idempotenceToggle.checked = true;
    const idempotence = producer && idempotenceToggle.checked;
    field("yaml-producer-acks").disabled = idempotence;
    if (idempotence) field("yaml-producer-acks").value = "all";
    field("yaml-producer-retries").min = idempotence ? "1" : "0";
    field("yaml-producer-max-in-flight").max = idempotence ? "5" : "2147483647";
    field("yaml-producer-transaction-prefix").disabled = !transaction;
    field("yaml-consumer-auto-commit").disabled = listener;
    if (listener) field("yaml-consumer-auto-commit").checked = false;

    const format = value("yaml-message-format");
    const knownClasses = classes[format];
    ["yaml-producer-value-serializer", "yaml-consumer-value-deserializer"].forEach((id, index) => {
      field(id).readOnly = Boolean(knownClasses);
      if (knownClasses) field(id).value = knownClasses[index];
    });
    form.querySelector("[data-json-contract]").hidden = format !== "json" || !consumer;
    form.querySelectorAll("[data-yaml-group]").forEach((group) => {
      const scope = group.dataset.yamlGroup;
      group.hidden = scope === "format" ? !producer && !consumer : !({ producer, consumer, listener })[scope];
    });
    form.querySelectorAll(".yaml-scope-card").forEach((card) => {
      card.classList.toggle("is-selected", card.querySelector("input").checked);
      card.classList.toggle("is-dependent", listener && card.contains(consumerToggle));
    });

    const selected = [];
    if (producer) selected.push("Producer");
    if (consumer) selected.push("Consumer Client");
    if (listener) selected.push("Spring Listener");
    scopeStatus.textContent = "현재 구성: " + (selected.join(" + ") || "선택 없음") + (listener ? " · Listener는 Consumer Client를 함께 사용합니다." : "");

    const errors = [];
    form.querySelectorAll("[aria-invalid]").forEach((input) => input.removeAttribute("aria-invalid"));
    form.querySelectorAll("[data-field-error]").forEach((message) => message.remove());
    function error(id, message) {
      errors.push(message);
      if (!id) return;
      const input = field(id);
      input.setAttribute("aria-invalid", "true");
      let help = field(id + "-error");
      if (!help) {
        help = document.createElement("small");
        help.id = id + "-error";
        help.dataset.fieldError = "";
        help.className = "yaml-field-error";
        input.closest("label").appendChild(help);
      }
      help.textContent = message;
      input.setAttribute("aria-describedby", help.id);
    }
    // Clear old error references before validating the current included scopes.
    form.querySelectorAll("[aria-describedby$='-error']").forEach((input) => input.removeAttribute("aria-describedby"));
    function required(id, label) {
      const input = value(id);
      if (!input) error(id, "필수 입력: " + label);
      else if (/[\r\n\u0000-\u001f]/.test(input)) error(id, label + "에 제어 문자를 사용할 수 없습니다.");
      else if (checked("yaml-use-env") && ["yaml-bootstrap-servers", "yaml-consumer-group-id", "yaml-producer-transaction-prefix"].includes(id) && /[${}]/.test(input)) {
        error(id, label + "에는 Placeholder 대신 기본값을 입력하세요. 환경변수 표기는 도구가 만듭니다.");
      }
      return input;
    }
    function number(id, label) {
      const input = field(id);
      const raw = input.value.trim();
      const parsed = Number(raw);
      const min = Number(input.min);
      const max = Number(input.max || Number.MAX_SAFE_INTEGER);
      if (!raw || !Number.isSafeInteger(parsed) || parsed < min || parsed > max) {
        error(id, label + " 입력 범위: " + min + "~" + max + " 사이의 정수");
        return null;
      }
      return parsed;
    }

    if (!producer && !consumer) error(null, "Producer 또는 Consumer 구성을 하나 이상 선택하세요.");
    const servers = required("yaml-bootstrap-servers", "Bootstrap Servers");
    if (servers && !servers.split(",").every((server) => {
      const match = server.trim().match(/^(?:\[[0-9a-fA-F:.]+\]|[^\s/:,\[\]?#]+):([0-9]+)$/);
      return match && Number(match[1]) >= 1 && Number(match[1]) <= 65535;
    })) error("yaml-bootstrap-servers", "Broker 주소는 host:port를 쉼표로 구분하세요. IPv6는 [주소]:port로 입력하세요.");

    const lines = ["# Kafka Study Notes · Spring Boot 3.x / Spring Kafka 3.3", "spring:", "  kafka:", "    bootstrap-servers: " + env("KAFKA_BOOTSTRAP_SERVERS", servers)];
    const messages = [];
    if (producer) {
      const retries = number("yaml-producer-retries", "retries");
      const linger = number("yaml-producer-linger", "linger.ms");
      const request = number("yaml-producer-request-timeout", "request.timeout.ms");
      const delivery = number("yaml-producer-delivery-timeout", "delivery.timeout.ms");
      const inFlight = number("yaml-producer-max-in-flight", "max.in.flight");
      if (request !== null && linger !== null && delivery !== null && delivery - request < linger) {
        error("yaml-producer-delivery-timeout", "delivery.timeout.ms는 request.timeout.ms + linger.ms 이상이어야 합니다.");
      }
      lines.push("    producer:", "      acks: " + scalar(value("yaml-producer-acks")), "      retries: " + retries,
        "      compression-type: " + scalar(value("yaml-producer-compression")),
        "      key-serializer: " + scalar(required("yaml-producer-key-serializer", "Key Serializer")),
        "      value-serializer: " + scalar(required("yaml-producer-value-serializer", "Value Serializer")),
        "      properties:", "        \"[enable.idempotence]\": " + idempotence,
        "        \"[linger.ms]\": " + linger, "        \"[request.timeout.ms]\": " + request,
        "        \"[delivery.timeout.ms]\": " + delivery, "        \"[max.in.flight.requests.per.connection]\": " + inFlight);
      if (format === "json") lines.push("        \"[spring.json.add.type.headers]\": false");
      if (transaction) {
        lines.push("      transaction-id-prefix: " + env("KAFKA_TRANSACTION_ID_PREFIX", required("yaml-producer-transaction-prefix", "Transaction ID Prefix")));
        messages.push("Transactional Producer: Prefix는 인스턴스마다 고유해야 합니다. Kafka Transaction을 시작하는 코드가 필요하며 외부 DB는 별도 경계입니다.");
      } else if (idempotence) messages.push("멱등성: acks=all · retries>0 · in-flight≤5. Producer의 전송 재시도 중복을 다룹니다.");
      else messages.push("멱등성이 꺼져 있습니다. Retry 중복과, in-flight>1일 때 재시도에 따른 순서 변경을 고려하세요.");
      if (value("yaml-producer-acks") === "0") messages.push("acks=0: Broker 저장을 확인하지 않으며 실제 전송 실패를 판단할 수 없어 재시도 보장도 기대할 수 없습니다.");
    }
    if (consumer) {
      const targetType = format === "json" ? required("yaml-json-target-type", "대상 DTO 클래스") : "";
      if (targetType && !/^(?:[A-Za-z_$][\w$]*\.)+[A-Za-z_$][\w$]*$/.test(targetType)) {
        error("yaml-json-target-type", "대상 DTO는 패키지를 포함한 클래스 이름으로 입력하세요. 예: com.example.OrderConfirmed");
      }
      lines.push("    consumer:", "      group-id: " + env("KAFKA_CONSUMER_GROUP", required("yaml-consumer-group-id", "Group ID")),
        "      key-deserializer: " + scalar(required("yaml-consumer-key-deserializer", "Key Deserializer")),
        "      value-deserializer: " + scalar(required("yaml-consumer-value-deserializer", "Value Deserializer")),
        "      enable-auto-commit: " + checked("yaml-consumer-auto-commit"),
        "      auto-offset-reset: " + scalar(value("yaml-consumer-offset-reset")),
        "      isolation-level: " + scalar(value("yaml-consumer-isolation")),
        "      max-poll-records: " + number("yaml-consumer-max-poll-records", "max.poll.records"),
        "      fetch-min-size: " + number("yaml-consumer-fetch-min-bytes", "fetch.min.bytes"),
        "      fetch-max-wait: " + number("yaml-consumer-fetch-max-wait", "fetch.max.wait.ms"),
        "      properties:", "        \"[max.poll.interval.ms]\": " + number("yaml-consumer-max-poll-interval", "max.poll.interval.ms"),
        "        \"[group.protocol]\": \"classic\"",
        "        \"[partition.assignment.strategy]\": " + scalar(value("yaml-consumer-assignment")));
      if (format === "json") {
        lines.push("        \"[spring.json.use.type.headers]\": false", "        \"[spring.json.value.default.type]\": " + scalar(targetType),
          "        \"[spring.json.trusted.packages]\": " + scalar(targetType.slice(0, targetType.lastIndexOf("."))));
        messages.push("JSON DTO: " + (targetType || "대상 타입 입력 필요") + " 클래스를 Consumer에 준비하세요. Type Header 대신 지정 타입을 사용합니다.");
      }
      if (value("yaml-consumer-isolation") === "read_committed") messages.push("read_committed: Commit된 Kafka Transaction과 비트랜잭션 Record를 읽습니다. Consumer 업무의 Exactly-once 보장은 별도입니다.");
      if (!listener) messages.push(checked("yaml-consumer-auto-commit")
        ? "Auto Commit은 외부 DB 완료를 확인하지 않습니다. 실제 처리와 Commit 시점을 함께 확인하세요."
        : "자동 Commit이 꺼져 있습니다. 직접 poll()하는 코드는 처리 후 Offset Commit과 실패 복구를 구현해야 합니다.");
    }
    if (listener) {
      lines.push("    listener:", "      concurrency: " + number("yaml-listener-concurrency", "concurrency"),
        "      ack-mode: " + scalar(value("yaml-listener-ack-mode")));
      messages.push(value("yaml-listener-ack-mode").startsWith("manual")
        ? "수동 Ack: DB Commit 성공 뒤 Listener 코드에서 acknowledge()해야 합니다. 실패 record의 error handler 정책도 정하세요."
        : "Container가 Ack Mode에 따라 Commit합니다. 예외를 삼키지 말고 업무 중복 방지와 error handler 정책을 함께 정하세요.");
    }
    if (format === "custom") messages.push("직접 지정: 클래스 존재 여부와 직렬화 호환성은 도구가 검사하지 않습니다. JSON 타입·신뢰 설정이 필요하면 별도로 추가하세요.");
    messages.push("Topic과 오류 처리기는 이 YAML에 포함되지 않습니다. 환경변수의 실제 값도 배포 환경에서 검증하세요.");
    showSummary(messages);
    valid = errors.length === 0;
    validation.classList.toggle("has-errors", !valid);
    validation.textContent = valid ? "입력 조건 확인 완료 · YAML을 복사하거나 저장할 수 있습니다." : errors.length + "개 입력 항목을 확인하세요. " + errors.join(" ");
    output.textContent = valid ? lines.join("\n") + "\n" : "# 입력 오류를 수정하면 YAML이 생성됩니다.\n# 오류 내용은 위 안내와 각 입력란에서 확인하세요.\n";
    exports.forEach((button) => { button.disabled = !valid; });
    exportStatus.textContent = "";
    updateYamlValueDescriptions();
  }

  presets.forEach((button) => button.addEventListener("click", () => {
    const name = button.dataset.yamlPreset;
    // Preserve the user's connection and data contract while changing operational scope.
    const preserved = Object.fromEntries(["yaml-bootstrap-servers", "yaml-message-format", "yaml-json-target-type", "yaml-producer-key-serializer", "yaml-producer-value-serializer", "yaml-consumer-key-deserializer", "yaml-consumer-value-deserializer", "yaml-consumer-group-id", "yaml-producer-transaction-prefix"].map((id) => [id, field(id).value]));
    const useEnv = checked("yaml-use-env");
    form.reset();
    Object.entries(preserved).forEach(([id, saved]) => { field(id).value = saved; });
    field("yaml-use-env").checked = useEnv;
    field("yaml-include-producer").checked = ["producer", "producer-eos", "service"].includes(name);
    field("yaml-include-consumer").checked = ["consumer", "consumer-eos", "service"].includes(name);
    field("yaml-include-listener").checked = name === "service";
    field("yaml-producer-transaction").checked = name === "producer-eos";
    field("yaml-consumer-isolation").value = name === "consumer-eos" ? "read_committed" : "read_uncommitted";
    field("yaml-listener-concurrency").value = name === "service" ? "3" : "1";
    form.querySelectorAll(".yaml-builder-group").forEach((group) => { group.open = true; });
    markPreset(name);
    update();
  }));
  function changed() {
    if (activePreset) markPreset("");
    update();
  }
  form.addEventListener("input", changed);
  form.addEventListener("change", changed);
  form.addEventListener("submit", (event) => event.preventDefault());
  form.querySelector("[data-builder-reset]").addEventListener("click", () => {
    form.reset();
    markPreset("");
    update();
  });
  form.querySelector("[data-builder-download]").addEventListener("click", () => {
    if (!valid) return;
    const url = URL.createObjectURL(new Blob([output.textContent], { type: "application/yaml;charset=utf-8" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = "application.yml";
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    exportStatus.textContent = "application.yml의 파일 저장을 요청했습니다.";
  });
  markPreset("");
  update();
})();
