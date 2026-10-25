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
      title: "Kafka Cluster",
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
      const active = link.dataset.chapterSwitch === (chapter === "control-plane" ? "cluster" : chapter);
      link.classList.toggle("is-active", active);
      if (active) {
        link.setAttribute("aria-current", "location");
      } else {
        link.removeAttribute("aria-current");
      }
    });

    const clusterSubnav = document.querySelector("[data-cluster-subnav]");
    if (clusterSubnav) {
      clusterSubnav.hidden = chapter !== "cluster" && chapter !== "control-plane";
      clusterSubnav.querySelectorAll("a").forEach(function (link) {
        const active = link.dataset.clusterView === chapter;
        link.classList.toggle("is-active", active);
        if (active) link.setAttribute("aria-current", "location");
        else link.removeAttribute("aria-current");
      });
    }
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
    const copied = document.execCommand("copy");
    textarea.remove();
    if (!copied) throw new Error("Copy failed");
  }

  document.querySelectorAll("[data-copy-target]").forEach(function (button) {
    button.addEventListener("click", async function () {
      const target = document.getElementById(button.dataset.copyTarget);
      if (!target || button.disabled) return;

      const originalLabel = button.textContent;
      try {
        await copyText(target.textContent);
        button.textContent = "복사됨";
        const status = document.querySelector("[data-builder-export-status]");
        if (status) status.textContent = "YAML을 복사했습니다.";
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
