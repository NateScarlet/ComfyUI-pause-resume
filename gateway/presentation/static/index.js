const EXT_NAMESPACE = "io.github.natescarlet.pause-resume";

const _locale = (function () {
  var lang = navigator.language || navigator.userLanguage || "";
  return lang.startsWith("zh") ? "zh" : "en";
})();

const _str = (function () {
  var t = {
    pauseBtn: _locale === "zh" ? "⏸️ 暂停" : "⏸️ Pause",
    resumeBtn: _locale === "zh" ? "▶️ 恢复" : "▶️ Resume",
    btnTitle:
      _locale === "zh"
        ? "Ctrl+点击：暂停并在闲置时重启"
        : "Ctrl+Click: Pause and restart when idle",
    cmdPause: _locale === "zh" ? "暂停队列" : "Pause Queue",
    cmdResume: _locale === "zh" ? "恢复队列" : "Resume Queue",
    cmdPauseRestart:
      _locale === "zh" ? "暂停并重启" : "Pause and Restart",
  };
  return function (key) {
    return t[key] || key;
  };
})();

const api = {
  baseUrl: `/${EXT_NAMESPACE}`,

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

const comfyApp = window.comfyAPI.app.app;

let paused = false;
let btnPause = null;

function setButtonState(btn) {
  if (!btn) return;
  btn.className =
    "relative inline-flex items-center justify-center gap-1.5 cursor-pointer touch-manipulation whitespace-nowrap appearance-none border-none font-medium font-inter transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50 h-8 rounded-lg p-2 text-xs px-3 " +
    (paused
      ? "bg-destructive-background text-base-foreground hover:bg-destructive-background-hover"
      : "bg-secondary-background text-secondary-foreground hover:bg-secondary-background-hover");
  btn.innerText = paused ? _str("resumeBtn") : _str("pauseBtn");
}

function createPauseButton() {
  const btn = document.createElement("button");
  btn.title = _str("btnTitle");
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

comfyApp.registerExtension({
  name: EXT_NAMESPACE,

  commands: [
    {
      id: `${EXT_NAMESPACE}.pause`,
      label: _str("cmdPause"),
      function: () => api.pause(),
    },
    {
      id: `${EXT_NAMESPACE}.resume`,
      label: _str("cmdResume"),
      function: () => api.resume(),
    },
    {
      id: `${EXT_NAMESPACE}.pause_and_restart`,
      label: _str("cmdPauseRestart"),
      function: () => api.pause(true),
    },
  ],

  async setup() {
    if (
      comfyApp.menu &&
      comfyApp.menu.settingsGroup &&
      comfyApp.menu.settingsGroup.element
    ) {
      btnPause = createPauseButton();
      btnPause.style.alignSelf = "center";
      comfyApp.menu.settingsGroup.element.appendChild(btnPause);
    }

    const eventSource = new EventSource(`${api.baseUrl}/sse`);
    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
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
