import { app } from "/scripts/app.js";

const EXT_NAMESPACE = "io.github.natescarlet.pause-resume";

// ─── 统一 API 调用层，避免重复 fetch 逻辑 ───────────────
const api = {
  baseUrl: `/${EXT_NAMESPACE}`,

  /** 通用 POST 请求，自动处理 JSON 序列化 */
  async _post(endpoint, body) {
    const opts = { method: "POST" };
    if (body !== undefined) {
      opts.headers = { "Content-Type": "application/json" };
      opts.body = JSON.stringify(body);
    }
    const resp = await fetch(`${this.baseUrl}/${endpoint}`, opts);
    return resp.json();
  },

  pause(restartAfterIdle) {
    if (restartAfterIdle) {
      return this._post("pause", { restart_after_idle: true });
    }
    return this._post("pause");
  },

  resume() {
    return this._post("resume");
  },
};

app.registerExtension({
  name: EXT_NAMESPACE,
  // ─── 注册命令，供 ComfyUI 快捷键系统使用 ───────────────
  commands: [
    {
      id: `${EXT_NAMESPACE}.pause`,
      label: "Pause Queue",
      function: () => api.pause(),
    },
    {
      id: `${EXT_NAMESPACE}.resume`,
      label: "Resume Queue",
      function: () => api.resume(),
    },
    {
      id: `${EXT_NAMESPACE}.pause_and_restart`,
      label: "Pause and Restart",
      function: () => api.pause(true),
    },
  ],
  async setup() {
    let paused = false;
    let btnPause = null;

    function setButtonState(btn) {
      if (!btn) return;
      const isNewUI = !!document.getElementById("vue-app");
      if (isNewUI) {
        btn.className =
          "relative inline-flex items-center justify-center gap-1.5 cursor-pointer touch-manipulation whitespace-nowrap appearance-none border-none font-medium font-inter transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50 h-8 rounded-lg p-2 text-xs px-3 " +
          (paused
            ? "bg-destructive-background text-base-foreground hover:bg-destructive-background-hover"
            : "bg-secondary-background text-secondary-foreground hover:bg-secondary-background-hover");
      } else {
        btn.className = "";
        btn.style.backgroundColor = "var(--bg-color)";
        btn.style.color = "var(--fg-color)";
        btn.style.border = paused ? "1px solid #e74c3c" : "1px solid #2ecc71";
      }
      btn.innerText = paused ? "▶️ Resume" : "⏸️ Pause";
    }

    function createPauseButton() {
      let btn = document.createElement("button");
      btn.title = "Ctrl+Click: Pause and restart when idle";
      btn.onclick = async (e) => {
        let data;
        if (paused) {
          data = await api.resume();
        } else {
          data = await api.pause(!e.shiftKey);
        }
        paused = data.paused;
        setButtonState(btn);
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

    const eventSource = new EventSource(`${api.baseUrl}/sse`);
    eventSource.onmessage = (event) => {
      try {
        let data = JSON.parse(event.data);
        paused = data.paused;
        if (btnPause) {
          setButtonState(btnPause);
        }
      } catch (e) {
        console.error("Error parsing SSE data", e);
      }
    };
  },
});
