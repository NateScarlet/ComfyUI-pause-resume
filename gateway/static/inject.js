import { app } from "/scripts/app.js";

const EXT_NAMESPACE = "io.github.natescarlet.pause-resume";

app.registerExtension({
  name: EXT_NAMESPACE,
  // ─── 注册命令，供 ComfyUI 快捷键系统使用 ───────────────
  commands: [
    {
      id: `${EXT_NAMESPACE}.pause`,
      label: "Pause Queue",
      function: async () => {
        await fetch(`/io.github.natescarlet.pause-resume/pause`, {
          method: "POST",
        });
      },
    },
    {
      id: `${EXT_NAMESPACE}.resume`,
      label: "Resume Queue",
      function: async () => {
        await fetch(`/io.github.natescarlet.pause-resume/resume`, {
          method: "POST",
        });
      },
    },
    {
      id: `${EXT_NAMESPACE}.pause_and_restart`,
      label: "Pause and Restart",
      function: async () => {
        await fetch(`/io.github.natescarlet.pause-resume/pause`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ restart_after_idle: true }),
        });
      },
    },
  ],
  async setup() {
    let proxyPaused = false;
    let btnPause = null;

    function setButtonState(btn) {
      if (!btn) return;
      const isNewUI = !!document.getElementById("vue-app");
      if (isNewUI) {
        btn.className =
          "relative inline-flex items-center justify-center gap-1.5 cursor-pointer touch-manipulation whitespace-nowrap appearance-none border-none font-medium font-inter transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50 h-8 rounded-lg p-2 text-xs px-3 " +
          (proxyPaused
            ? "bg-destructive-background text-base-foreground hover:bg-destructive-background-hover"
            : "bg-secondary-background text-secondary-foreground hover:bg-secondary-background-hover");
      } else {
        btn.className = "";
        btn.style.backgroundColor = "var(--bg-color)";
        btn.style.color = "var(--fg-color)";
        btn.style.border = proxyPaused
          ? "1px solid #e74c3c"
          : "1px solid #2ecc71";
      }
      btn.innerText = proxyPaused ? "▶️ Resume" : "⏸️ Pause";
    }

    function createPauseButton() {
      let btn = document.createElement("button");
      btn.title = "Ctrl+Click: Pause and restart when idle";
      btn.onclick = async (e) => {
        const ctrlPressed = e.ctrlKey || e.metaKey;
        if (proxyPaused) {
          let resp = await fetch(`/io.github.natescarlet.pause-resume/resume`, {
            method: "POST",
          });
          let data = await resp.json();
          proxyPaused = data.paused;
          setButtonState(btn);
        } else {
          let body = ctrlPressed
            ? JSON.stringify({ restart_after_idle: true })
            : undefined;
          let opts = { method: "POST" };
          if (body) {
            opts.headers = { "Content-Type": "application/json" };
            opts.body = body;
          }
          let resp = await fetch(
            `/io.github.natescarlet.pause-resume/pause`,
            opts
          );
          let data = await resp.json();
          proxyPaused = data.paused;
          setButtonState(btn);
        }
      };

      setButtonState(btn);
      return btn;
    }

    const isNewUI = !!document.getElementById("vue-app");

    if (isNewUI) {
      if (
        app.menu &&
        app.menu.settingsGroup &&
        app.menu.settingsGroup.element
      ) {
        btnPause = createPauseButton();
        btnPause.style.alignSelf = "center";
        app.menu.settingsGroup.element.appendChild(btnPause);
      }
    } else {
      let qMenu = document.querySelector(".comfy-menu");
      if (qMenu) {
        btnPause = createPauseButton();
        btnPause.style.marginTop = "4px";
        qMenu.appendChild(btnPause);
      }
    }

    const eventSource = new EventSource("/io.github.natescarlet.pause-resume/sse");
    eventSource.onmessage = (event) => {
      try {
        let data = JSON.parse(event.data);
        proxyPaused = data.paused;
        if (btnPause) {
          setButtonState(btnPause);
        }
      } catch (e) {
        console.error("Error parsing SSE data", e);
      }
    };
  },
});
