(function () {
  "use strict";

  const progressBar = document.getElementById("reading-progress-bar");
  const sections = Array.from(document.querySelectorAll("main section[id]"));
  const tocLinks = Array.from(document.querySelectorAll(".toc-chapter a"));
  const tocChapters = Array.from(document.querySelectorAll(".toc-chapter"));
  const chapterSwitches = Array.from(document.querySelectorAll("[data-chapter-switch]"));
  const chapterNumber = document.querySelector("[data-chapter-number]");
  const chapterTitle = document.querySelector("[data-chapter-title]");
  const chapterDescription = document.querySelector("[data-chapter-description]");
  const mobileNav = document.querySelector(".mobile-nav");
  const toolSwitches = Array.from(document.querySelectorAll("[data-tool-switch]"));
  const chapterMeta = {
    overview: {
      number: "00",
      title: "Kafka 기초",
      description: "왜 Kafka인가 · Event · Log 모델 · 용어 사전"
    },
    "data-plane": {
      number: "01",
      title: "Data Plane",
      description: "Producer → Broker → Consumer와 Delivery Guarantees"
    },
    cluster: {
      number: "02",
      title: "Kafka Cluster",
      description: "Broker · 저장과 복제 · Topic 설계 · 운영 지표"
    },
    "control-plane": {
      number: "02",
      title: "Control Plane",
      description: "Kafka Cluster의 Metadata 관리 · Controller · KRaft · ZooKeeper"
    },
    spring: {
      number: "03",
      title: "Spring Kafka",
      description: "KafkaTemplate · KafkaListener · Container 구성"
    }
  };

  function chapterForSection(id) {
    if (!id || id === "top" || id.indexOf("intro-") === 0) return "overview";

    if (id.indexOf("spring-") === 0) {
      return "spring";
    }

    if (id === "cluster-controller" || id.indexOf("control-plane-") === 0 || id.indexOf("kraft-") === 0 || id.indexOf("zookeeper-") === 0) {
      return "control-plane";
    }

    if (id.indexOf("broker-") === 0 || id.indexOf("cluster-") === 0) {
      return "cluster";
    }

    return "data-plane";
  }

  function setChapter(chapter) {
    const meta = chapterMeta[chapter];
    if (!meta) return;

    tocChapters.forEach(function (toc) {
      toc.hidden = toc.dataset.chapter !== chapter;
    });

    chapterSwitches.forEach(function (link) {
      const active = link.dataset.chapterSwitch === chapter;
      link.classList.toggle("is-active", active);
      if (active) {
        link.setAttribute("aria-current", "location");
      } else {
        link.removeAttribute("aria-current");
      }
    });

    if (chapterNumber) chapterNumber.textContent = meta.number;
    if (chapterTitle) chapterTitle.textContent = meta.title;
    if (chapterDescription) chapterDescription.textContent = meta.description;
  }

  function setBuilderMode(active) {
    document.body.classList.toggle("builder-mode", active);
    toolSwitches.forEach(function (link) {
      link.classList.toggle("is-active", active);
      if (active) {
        link.setAttribute("aria-current", "page");
      } else {
        link.removeAttribute("aria-current");
      }
    });
  }

  function updateProgress() {
    if (!progressBar) return;
    const scrollTop = window.scrollY || document.documentElement.scrollTop;
    const scrollable = document.documentElement.scrollHeight - window.innerHeight;
    const progress = scrollable > 0 ? Math.min(100, Math.max(0, (scrollTop / scrollable) * 100)) : 0;
    progressBar.style.width = progress + "%";
  }

  function setActiveSection(id) {
    tocLinks.forEach(function (link) {
      const active = link.getAttribute("href") === "#" + id;
      link.classList.toggle("is-active", active);
      if (active) {
        link.setAttribute("aria-current", "location");
      } else {
        link.removeAttribute("aria-current");
      }
    });
    setBuilderMode(id === "config-builder");
  }

  let activeSectionId = "";
  let sectionSyncQueued = false;

  function syncSectionToScroll() {
    const topbar = document.querySelector(".topbar");
    const marker = (topbar ? topbar.offsetHeight : 0) + Math.min(96, window.innerHeight * 0.18);
    let id = "top";

    for (let index = 0; index < sections.length; index += 1) {
      if (sections[index].getBoundingClientRect().top > marker) break;
      id = sections[index].id;
    }

    if (id !== activeSectionId) {
      activeSectionId = id;
      setChapter(chapterForSection(id));
      setActiveSection(id);
    }
    sectionSyncQueued = false;
  }

  function queueSectionSync() {
    if (sectionSyncQueued) return;
    sectionSyncQueued = true;
    window.requestAnimationFrame(syncSectionToScroll);
  }

  window.addEventListener("scroll", queueSectionSync, { passive: true });
  window.addEventListener("resize", queueSectionSync);

  chapterSwitches.forEach(function (link) {
    link.addEventListener("click", function () {
      setChapter(link.dataset.chapterSwitch);
    });
  });

  toolSwitches.forEach(function (link) {
    link.addEventListener("click", function () {
      setBuilderMode(true);
    });
  });

  function syncChapterToHash() {
    if (!window.location.hash) return;

    const id = window.location.hash.slice(1);
    setChapter(chapterForSection(id));
    setActiveSection(id);
  }

  syncChapterToHash();
  queueSectionSync();
  window.addEventListener("hashchange", syncChapterToHash);

  async function copyText(text) {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return;
    }

    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "");
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand("copy");
    textarea.remove();
  }

  const yamlBuilder = document.querySelector("[data-yaml-builder]");

  if (yamlBuilder) {
    const field = function (id) { return document.getElementById(id); };
    const value = function (id) { return field(id).value.trim(); };
    const isChecked = function (id) { return field(id).checked; };
    const builderStatus = yamlBuilder.parentElement.querySelector("[data-builder-status]");
    const builderScopeStatus = yamlBuilder.parentElement.querySelector("[data-builder-scope-status]");
    const presetButtons = Array.from(yamlBuilder.parentElement.querySelectorAll("[data-yaml-preset]"));
    const presetLabels = {
      producer: "Producer · 안정성 기본",
      "producer-eos": "Producer · Transaction 준비",
      consumer: "Consumer · At-least-once 기반",
      "consumer-eos": "Consumer · Commit된 Transaction만 읽기",
      service: "Producer + Consumer"
    };
    const producerPresets = new Set([
      "producer",
      "producer-eos",
      "service"
    ]);
    const consumerPresets = new Set([
      "consumer",
      "consumer-eos",
      "service"
    ]);
    const listenerPresets = new Set([
      "service"
    ]);
    const scalar = function (input) { return JSON.stringify(String(input)); };
    const numberValue = function (id, fallback) {
      const input = field(id);
      const raw = input.value.trim();
      const parsed = Number(raw);
      const minimum = Number(input.min || "0");
      const maximum = input.max ? Number(input.max) : Number.MAX_SAFE_INTEGER;
      return raw !== "" && Number.isSafeInteger(parsed) && parsed >= minimum && parsed <= maximum
        ? String(parsed)
        : String(fallback);
    };
    const envScalar = function (name, fallback, enabled) {
      return scalar(enabled ? "${" + name + ":" + fallback + "}" : fallback);
    };

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
      yamlBuilder.querySelectorAll("[data-yaml-value-help]").forEach(function (help) {
        const id = help.dataset.yamlValueHelp;
        const input = field(id);
        const descriptions = yamlValueDescriptions[id];
        help.textContent = input && descriptions ? (descriptions[input.value] || "") : "";
      });

      yamlBuilder.querySelectorAll("[data-yaml-choice-guide]").forEach(function (guide) {
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

    function updateYamlBuilder() {
      const useEnv = isChecked("yaml-use-env");
      const includeProducer = isChecked("yaml-include-producer");
      const includeListener = isChecked("yaml-include-listener");
      const consumerScopeToggle = field("yaml-include-consumer");

      // Spring Listener Container는 내부적으로 KafkaConsumer를 사용하므로
      // Listener를 선택하면 Consumer Client 설정을 함께 보여주고 해제를 잠근다.
      consumerScopeToggle.disabled = includeListener;
      consumerScopeToggle.checked = includeListener || consumerScopeToggle.checked;
      const includeConsumer = consumerScopeToggle.checked || includeListener;
      const autoCommitToggle = field("yaml-consumer-auto-commit");
      // AckMode belongs to the container; manual AckMode rejects auto commit.
      autoCommitToggle.disabled = includeListener;
      if (includeListener) autoCommitToggle.checked = false;
      const transactionEnabled = isChecked("yaml-producer-transaction");
      const idempotence = transactionEnabled || isChecked("yaml-producer-idempotence");
      const producerAcks = field("yaml-producer-acks");
      const producerRetries = field("yaml-producer-retries");

      if (idempotence) {
        producerAcks.value = "all";
        producerAcks.disabled = true;
        producerRetries.min = "1";
        if (Number(producerRetries.value) < 1) producerRetries.value = "1";
      } else {
        producerAcks.disabled = false;
        producerRetries.min = "0";
      }

      const lines = [
        "# Generated by Kafka Study Notes",
        "# Review Broker address, serializers, Group ID, and security settings before using in production.",
        "spring:",
        "  kafka:",
        "    bootstrap-servers: " + envScalar("KAFKA_BOOTSTRAP_SERVERS", value("yaml-bootstrap-servers") || "localhost:9092", useEnv)
      ];

      if (includeProducer) {
        lines.push("    producer:");
        lines.push("      acks: " + scalar(value("yaml-producer-acks")));
        lines.push("      retries: " + numberValue("yaml-producer-retries", 10));
        lines.push("      compression-type: " + scalar(value("yaml-producer-compression")));
        lines.push("      key-serializer: " + scalar(value("yaml-producer-key-serializer")));
        lines.push("      value-serializer: " + scalar(value("yaml-producer-value-serializer")));
        lines.push("      properties:");
        lines.push("        \"[enable.idempotence]\": " + String(idempotence));
        lines.push("        \"[linger.ms]\": " + numberValue("yaml-producer-linger", 5));

        if (transactionEnabled) {
          lines.push("      transaction-id-prefix: " + envScalar("KAFKA_TRANSACTION_ID_PREFIX", value("yaml-producer-transaction-prefix") || "order-tx-", useEnv));
        }
      }

      if (includeConsumer) {
        lines.push("    consumer:");
        lines.push("      group-id: " + envScalar("KAFKA_CONSUMER_GROUP", value("yaml-consumer-group-id") || "order-service-v1", useEnv));
        lines.push("      key-deserializer: " + scalar(value("yaml-consumer-key-deserializer")));
        lines.push("      value-deserializer: " + scalar(value("yaml-consumer-value-deserializer")));
        lines.push("      enable-auto-commit: " + String(isChecked("yaml-consumer-auto-commit")));
        lines.push("      auto-offset-reset: " + scalar(value("yaml-consumer-offset-reset")));
        lines.push("      isolation-level: " + scalar(value("yaml-consumer-isolation")));
        lines.push("      max-poll-records: " + numberValue("yaml-consumer-max-poll-records", 100));
        lines.push("      max-poll-interval: " + numberValue("yaml-consumer-max-poll-interval", 300000));
        lines.push("      fetch-min-size: " + numberValue("yaml-consumer-fetch-min-bytes", 1));
        lines.push("      fetch-max-wait: " + numberValue("yaml-consumer-fetch-max-wait", 500));
        lines.push("      properties:");
        lines.push("        \"[partition.assignment.strategy]\": " + scalar(value("yaml-consumer-assignment")));
      }

      if (includeListener) {
        lines.push("    listener:");
        lines.push("      concurrency: " + numberValue("yaml-listener-concurrency", 1));
        lines.push("      ack-mode: " + scalar(value("yaml-listener-ack-mode")));
      }

      field("yaml-producer-idempotence").disabled = transactionEnabled;
      field("yaml-producer-idempotence").checked = idempotence;
      field("yaml-producer-transaction-prefix").disabled = !transactionEnabled;
      yamlBuilder.querySelectorAll(".yaml-scope-card").forEach(function (card) {
        const toggle = card.querySelector("input");
        card.classList.toggle("is-selected", Boolean(toggle && toggle.checked));
        card.classList.toggle("is-dependent", card.contains(consumerScopeToggle) && includeListener);
      });
      yamlBuilder.querySelector('[data-yaml-group="producer"]').hidden = !includeProducer;
      yamlBuilder.querySelector('[data-yaml-group="consumer"]').hidden = !includeConsumer;
      yamlBuilder.querySelector('[data-yaml-group="listener"]').hidden = !includeListener;
      field("yaml-output").textContent = lines.join("\n") + "\n";
      if (builderScopeStatus) {
        const selected = [];
        if (includeProducer) selected.push("Producer");
        if (includeConsumer) selected.push("Consumer Client");
        if (includeListener) selected.push("Spring Listener");
        builderScopeStatus.textContent = includeListener
          ? "현재 구성: " + selected.join(" + ") + " · Listener가 Consumer Client 설정을 자동 포함합니다."
          : "현재 구성: " + (selected.length ? selected.join(" + ") : "선택 없음") + " · 필요한 계층만 선택했습니다.";
      }
      updateYamlValueDescriptions();
    }

    let activePreset = "";

    function setValue(id, nextValue) {
      field(id).value = nextValue;
    }

    function setChecked(id, checked) {
      field(id).checked = checked;
    }

    function markPreset(name) {
      activePreset = name;
      presetButtons.forEach(function (button) {
        button.classList.toggle("is-active", button.dataset.yamlPreset === name);
      });
      if (builderStatus) {
        builderStatus.textContent = "현재 프리셋: " + presetLabels[name] + " · 설정은 기반일 뿐 실제 보장은 처리·Commit 코드까지 함께 결정합니다.";
      }
    }

    function applyPreset(name) {
      yamlBuilder.reset();
      yamlBuilder.querySelectorAll(".yaml-builder-group").forEach(function (group) {
        group.open = true;
      });

      setChecked("yaml-include-producer", producerPresets.has(name));
      setChecked("yaml-include-consumer", consumerPresets.has(name));
      setChecked("yaml-include-listener", listenerPresets.has(name));
      setChecked("yaml-producer-idempotence", true);
      setChecked("yaml-producer-transaction", name === "producer-eos");
      setChecked("yaml-consumer-auto-commit", false);
      setValue("yaml-consumer-offset-reset", "earliest");
      setValue("yaml-consumer-isolation", name === "consumer-eos" ? "read_committed" : "read_uncommitted");
      setValue("yaml-listener-ack-mode", "record");
      setValue("yaml-listener-concurrency", name === "service" ? "3" : "1");

      markPreset(name);
      updateYamlBuilder();
    }

    presetButtons.forEach(function (button) {
      button.addEventListener("click", function () {
        applyPreset(button.dataset.yamlPreset);
      });
    });

    function handleManualChange() {
      if (activePreset) {
        activePreset = "";
        presetButtons.forEach(function (button) { button.classList.remove("is-active"); });
        if (builderStatus) builderStatus.textContent = "직접 옵션을 조정하고 있습니다.";
      }
      updateYamlBuilder();
    }

    yamlBuilder.addEventListener("input", handleManualChange);
    yamlBuilder.addEventListener("change", handleManualChange);
    yamlBuilder.addEventListener("submit", function (event) { event.preventDefault(); });

    const resetBuilder = yamlBuilder.querySelector("[data-builder-reset]");
    if (resetBuilder) {
      resetBuilder.addEventListener("click", function () {
        yamlBuilder.reset();
        activePreset = "";
        presetButtons.forEach(function (button) { button.classList.remove("is-active"); });
        if (builderStatus) builderStatus.textContent = "프리셋은 설정 기반을 만듭니다. 실제 전달 보장은 전송·처리·Commit 코드까지 함께 결정합니다.";
        updateYamlBuilder();
      });
    }

    updateYamlBuilder();
  }

  document.querySelectorAll("[data-copy-target]").forEach(function (button) {
    button.addEventListener("click", async function () {
      const target = document.getElementById(button.dataset.copyTarget);
      if (!target) return;

      const originalLabel = button.textContent;
      try {
        await copyText(target.textContent);
        button.textContent = "복사됨";
      } catch (error) {
        button.textContent = "실패";
      }

      window.setTimeout(function () {
        button.textContent = originalLabel;
      }, 1400);
    });
  });

  if (mobileNav) {
    mobileNav.querySelectorAll("a").forEach(function (link) {
      link.addEventListener("click", function () {
        mobileNav.removeAttribute("open");
      });
    });
  }

  window.addEventListener("scroll", updateProgress, { passive: true });
  window.addEventListener("resize", updateProgress);
  updateProgress();
})();
