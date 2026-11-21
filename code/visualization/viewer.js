const byId = (id) => document.getElementById(id);
const symbolSelect = byId("symbol");
const chart = byId("chart");
const plot = byId("plot");
const markers = byId("markers");
const marketZone = { timeZone: "Europe/Paris", hourCycle: "h23" };
const marketDay = new Intl.DateTimeFormat("en-GB", { ...marketZone, dateStyle: "medium" });
const marketClock = new Intl.DateTimeFormat("en-GB", {
  ...marketZone, hour: "2-digit", minute: "2-digit",
});
const marketDateTick = new Intl.DateTimeFormat("en-GB", {
  ...marketZone, day: "numeric", month: "short", hour: "2-digit", minute: "2-digit",
});
const marketDateTime = new Intl.DateTimeFormat("en-GB", {
  ...marketZone, dateStyle: "medium", timeStyle: "short",
});
const axisNumber = new Intl.NumberFormat("en", { maximumSignificantDigits: 5 });
const detailNumber = new Intl.NumberFormat("en", { maximumSignificantDigits: 12 });
const markerButtons = new Map();
const seriesNames = { ema38: "EMA38", ema100: "EMA100" };
const previousCells = document.querySelectorAll("[data-previous]");

let view = null;
let selectedSymbol = "";
let selectedMarker = "";
let selectedPoint = null;
let plottedPoints = [];
let previousBody = "";
let requestId = 0;
let timer;

function setStatus(text) {
  const status = byId("status");
  if (status.textContent !== text) status.textContent = text;
}

function updateSymbols(symbols) {
  const currentSymbols = Array.from(symbolSelect.options, (option) => option.value).filter(Boolean);
  if (currentSymbols.join(",") !== symbols.join(",")) {
    const options = symbols.map((symbol) => new Option(symbol, symbol));
    symbolSelect.replaceChildren(...(options.length ? options : [new Option("Waiting for symbols", "")]));
  }
  symbolSelect.disabled = symbols.length === 0;
  symbolSelect.value = selectedSymbol;
}

function showDetails(result, field) {
  byId("details").hidden = !result;
  if (!result) return;
  const isAdvisory = !field;
  byId("details-title").textContent = `${field ? seriesNames[field] : result.kind} · ${view.symbol}`;
  byId("details-time").textContent = `${marketDateTime.format(result.time)} market time`;
  byId("details-reason").hidden = !isAdvisory;
  for (const cell of previousCells) cell.hidden = !isAdvisory;
  if (isAdvisory) {
    byId("details-reason").textContent = result.reason.replaceAll("_", " ");
    byId("previous38").textContent = detailNumber.format(result.previous38);
    byId("previous100").textContent = detailNumber.format(result.previous100);
  }
  byId("current38").textContent = detailNumber.format(result.ema38);
  byId("current100").textContent = detailNumber.format(result.ema100);
  byId("close").textContent = detailNumber.format(result.close);
  byId("details-ema38").classList.toggle("selected", field === "ema38");
  byId("details-ema100").classList.toggle("selected", field === "ema100");
}

function drawSeries(context, windows, x, y, field, color, dash = []) {
  context.strokeStyle = color;
  context.fillStyle = color;
  context.lineWidth = 2;
  context.lineJoin = "round";
  context.setLineDash(dash);
  context.beginPath();
  context.moveTo(x(0), y(windows[0][field]));
  for (let index = 1; index < windows.length; index++) {
    context.lineTo(x(index), y(windows[index][field]));
  }
  context.stroke();
  context.setLineDash([]);

  context.beginPath();
  windows.forEach((point, index) => {
    const pointX = x(index);
    const pointY = y(point[field]);
    plottedPoints.push({ point, field, x: pointX, y: pointY, color });
    context.moveTo(pointX + 2, pointY);
    context.arc(pointX, pointY, 2, 0, Math.PI * 2);
  });
  context.fill();
}

function drawMarkers(x, y) {
  const windowIndexes = new Map(view.windows.map((point, index) => [point.time, index]));
  const activeKeys = new Set();
  let activeAdvisory;
  for (const advisory of view.advisories) {
    const index = windowIndexes.get(advisory.time);
    if (index === undefined) continue;
    const key = `${view.symbol}:${advisory.time}:${advisory.kind}`;
    activeKeys.add(key);
    let button = markerButtons.get(key);
    if (!button) {
      button = document.createElement("button");
      button.type = "button";
      button.className = `marker ${advisory.kind.toLowerCase()}`;
      button.title = `${advisory.kind} · ${marketDateTime.format(advisory.time)}`;
      button.setAttribute("aria-label", button.title);
      const shape = document.createElement("span");
      shape.className = "triangle";
      button.append(shape);
      button.addEventListener("click", () => {
        selectedMarker = key;
        selectedPoint = null;
        drawChart();
      });
      markers.append(button);
      markerButtons.set(key, button);
    }
    button.style.left = `${x(index)}px`;
    button.style.top = `${y(view.windows[index].ema38)}px`;
    button.setAttribute("aria-pressed", String(key === selectedMarker));
    if (key === selectedMarker) activeAdvisory = advisory;
  }
  for (const [key, button] of markerButtons) {
    if (!activeKeys.has(key)) {
      button.remove();
      markerButtons.delete(key);
    }
  }
  return activeAdvisory;
}

function pointAt(event) {
  const { left, top } = plot.getBoundingClientRect();
  const x = event.clientX - left;
  const y = event.clientY - top;
  let nearest;
  let closestSquared = 12 ** 2;
  for (const candidate of plottedPoints) {
    const distanceSquared = (candidate.x - x) ** 2 + (candidate.y - y) ** 2;
    if (distanceSquared <= closestSquared) {
      nearest = candidate;
      closestSquared = distanceSquared;
    }
  }
  return nearest;
}

function drawChart() {
  if (!view?.windows.length) return;
  const { windows } = view;
  const { width, height } = chart.getBoundingClientRect();
  if (!width || !height) return;
  const scale = window.devicePixelRatio || 1;
  plot.width = Math.round(width * scale);
  plot.height = Math.round(height * scale);
  const context = plot.getContext("2d");
  context.scale(scale, scale);
  context.font = "14px system-ui";

  const lastIndex = windows.length - 1;
  const days = windows.map((point) => marketDay.format(point.time));
  const sameDay = days[0] === days[lastIndex];
  const formatTime = sameDay ? marketClock : marketDateTick;
  let low = Infinity;
  let high = -Infinity;
  for (const { ema38, ema100 } of windows) {
    low = Math.min(low, ema38, ema100);
    high = Math.max(high, ema38, ema100);
  }
  const padding = high > low ? (high - low) * 0.1 : Math.max(Math.abs(high) * 0.05, 0.000001);
  low -= padding;
  high += padding;

  const yTicks = Array.from({ length: 5 }, (_, index) => low + (high - low) * index / 4);
  const labelWidth = Math.max(...yTicks.map((value) => context.measureText(axisNumber.format(value)).width));
  const timeWidth = context.measureText(sameDay ? "00:00" : "00 Sept, 00:00").width;
  const left = Math.max(labelWidth + 12, timeWidth / 2);
  const right = width - Math.max(18, timeWidth / 2);
  const top = 18;
  const bottom = height - 26;
  // Give each finalized window the same spacing, including across day changes.
  const x = (index) => lastIndex ? left + index / lastIndex * (right - left) : (left + right) / 2;
  const y = (value) => bottom - (value - low) / (high - low) * (bottom - top);

  const theme = getComputedStyle(chart);
  context.strokeStyle = theme.getPropertyValue("--border").trim();
  context.fillStyle = theme.getPropertyValue("--muted").trim();
  context.textAlign = "right";
  context.textBaseline = "middle";
  for (const value of yTicks) {
    const gridY = y(value);
    context.beginPath();
    context.moveTo(left, gridY);
    context.lineTo(right, gridY);
    context.stroke();
    context.fillText(axisNumber.format(value), left - 7, gridY);
  }
  if (!sameDay) {
    context.setLineDash([3, 4]);
    context.beginPath();
    for (let index = 1; index <= lastIndex; index++) {
      if (days[index] === days[index - 1]) continue;
      const divider = x(index - 0.5);
      context.moveTo(divider, top);
      context.lineTo(divider, bottom);
    }
    context.stroke();
    context.setLineDash([]);
  }
  context.textAlign = "center";
  const intervals = Math.min(lastIndex, Math.max(1, Math.min(6, Math.floor((right - left) / (timeWidth + 14)))));
  for (let tick = 0; tick <= intervals; tick++) {
    const index = intervals ? Math.round(lastIndex * tick / intervals) : 0;
    context.fillText(formatTime.format(windows[index].time), x(index), bottom + 14);
  }

  plottedPoints = [];
  drawSeries(context, windows, x, y, "ema100", theme.getPropertyValue("--ema100").trim(), [7, 4]);
  drawSeries(context, windows, x, y, "ema38", theme.getPropertyValue("--ema38").trim());
  const advisory = drawMarkers(x, y);
  const activePoint = selectedPoint && plottedPoints.find(({ point, field }) =>
    point.time === selectedPoint.time && field === selectedPoint.field
  );
  if (activePoint) {
    context.beginPath();
    context.strokeStyle = activePoint.color;
    context.arc(activePoint.x, activePoint.y, 6, 0, Math.PI * 2);
    context.stroke();
  }
  showDetails(activePoint?.point ?? advisory, activePoint?.field);
}

function showView(results) {
  if (results.symbol !== view?.symbol) {
    selectedMarker = "";
    selectedPoint = null;
  }
  view = results;
  selectedSymbol = view.symbol;
  byId("run-name").textContent = view.run;
  updateSymbols(view.symbols);
  const hasResults = view.windows.length > 0;
  byId("empty").hidden = hasResults;
  byId("market-time").hidden = !hasResults;
  chart.hidden = !hasResults;
  if (hasResults) {
    drawChart();
  } else {
    markers.replaceChildren();
    markerButtons.clear();
    plottedPoints = [];
    showDetails(null);
  }
}

async function refresh() {
  clearTimeout(timer);
  const id = ++requestId;
  try {
    const query = new URLSearchParams({ symbol: selectedSymbol });
    const response = await fetch(`/api/results?${query}`, { cache: "no-store" });
    if (!response.ok) throw new Error("Could not load results");
    const body = await response.text();
    if (id !== requestId) return;
    if (body !== previousBody) {
      showView(JSON.parse(body));
      previousBody = body;
    }
    const count = view.windows.length;
    setStatus(count ? `${count} finalized ${count === 1 ? "window" : "windows"}` : "Waiting for results");
  } catch {
    if (id === requestId) setStatus("Unable to read results. Retrying…");
  } finally {
    if (id === requestId) timer = setTimeout(refresh, 1000);
  }
}

symbolSelect.addEventListener("change", () => {
  selectedSymbol = symbolSelect.value;
  refresh();
});
plot.addEventListener("pointermove", (event) => plot.classList.toggle("point-hover", Boolean(pointAt(event))));
plot.addEventListener("click", (event) => {
  const hit = pointAt(event);
  if (!hit) return;
  selectedMarker = "";
  selectedPoint = { time: hit.point.time, field: hit.field };
  drawChart();
});
new ResizeObserver(drawChart).observe(chart);
refresh();
