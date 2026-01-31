// Change this only if your Worker URL changes:
const API_URL = "https://blacksignals-api.princecampbellnyc.workers.dev/";

const elUrl = document.getElementById("apiUrl");
const elStatus = document.getElementById("apiStatus");
const elOut = document.getElementById("out");
const elTxt = document.getElementById("txt");

const btnPing = document.getElementById("btnPing");
const btnPost = document.getElementById("btnPost");

elUrl.textContent = API_URL;

function setStatus(text) {
  elStatus.textContent = text;
}

function show(obj) {
  elOut.textContent = typeof obj === "string" ? obj : JSON.stringify(obj, null, 2);
}

async function ping() {
  setStatus("API: checking…");
  show("Pinging API…");

  try {
    const res = await fetch(API_URL, { method: "GET" });
    const data = await res.json().catch(() => ({}));

    if (!res.ok) {
      setStatus("API: bad");
      show({ error: "GET failed", status: res.status, data });
      return;
    }

    setStatus("API: ok");
    show(data);
  } catch (err) {
    setStatus("API: unreachable");
    show({ error: "Network error", details: String(err) });
  }
}

async function postTest() {
  const text = (elTxt.value || "").trim();
  if (!text) {
    show("Type or paste something first.");
    return;
  }

  setStatus("API: sending…");
  show("Sending POST…");

  try {
    const res = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text })
    });

    const data = await res.json().catch(() => ({}));

    if (!res.ok) {
      setStatus("API: bad");
      show({ error: "POST failed", status: res.status, data });
      return;
    }

    setStatus("API: ok");
    show(data);
  } catch (err) {
    setStatus("API: unreachable");
    show({ error: "Network error", details: String(err) });
  }
}

btnPing.addEventListener("click", ping);
btnPost.addEventListener("click", postTest);

// Optional: auto-ping once when page loads
ping();
