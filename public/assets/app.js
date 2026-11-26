const byId = (id) => document.getElementById(id);
const escapeHtml = (value) => String(value ?? "—").replace(/[&<>"']/g, (char) => (
  { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]
));
const number = new Intl.NumberFormat(undefined);
const integer = (value) => value == null ? "—" : number.format(value);
const percent = (value, digits = 2) => value == null ? "—" : value.toFixed(digits) + "%";
const pp = (value) => value == null ? "—" : (value > 0 ? "+" : "") + value.toFixed(2) + " pp";
const color = (value) => value < 0 ? "loss" : value > 0 ? "gain" : "";
const dateText = (value) => value ? new Intl.DateTimeFormat("en-GB", {
  timeZone: "UTC", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hourCycle: "h23",
}).format(new Date(value)) + " UTC" : "—";
const verdicts = { review_candidate: "Human review candidate", inconclusive: "Inconclusive", hold: "Hold", invalid: "Invalid readout" };
const sources = new Map();
const requests = new Map();
let lastReport = null;
let active = "incident";
let chartObserver;

async function request(path, options = {}) {
  const response = await fetch(path, { ...options, signal: options.signal ?? AbortSignal.timeout(25000) });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(typeof body.detail === "string" ? body.detail : "Request failed (" + response.status + ")");
  }
  return response.json();
}

function setStatus(view, message, error = false) {
  const status = byId(view + "-status");
  status.textContent = message;
  status.classList.toggle("error", error);
}

function stat(title, value, detail, tone = "") {
  return `<div class="stat"><span>${escapeHtml(title)}</span><strong class="${tone}">${escapeHtml(value)}</strong>
    <small>${escapeHtml(detail)}</small></div>`;
}

function table(headers, rows, numeric = [], label = "") {
  return `<div class="table-wrap"><table aria-label="${escapeHtml(label)}"><thead><tr>${headers.map((text, i) =>
    `<th scope="col" class="${numeric.includes(i) ? "num" : ""}">${escapeHtml(text)}</th>`).join("")}
    </tr></thead><tbody>${rows.map((row) => `<tr>${row.map((text, i) =>
    `<td class="${numeric.includes(i) ? "num" : ""}">${escapeHtml(text)}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
}

function sourceStatus(data) {
  const scope = data.report.windows;
  const date = dateText(scope?.observed_through ?? data.report.observed_through);
  const coverage = { fixture: "Fixture coverage", attested: "Source coverage attested",
    latest_event_only: "Latest event only · source coverage unverified" }[data.source.coverage];
  return `Data through ${date} · ${coverage} · ${Math.round(data.duration_ms)} ms`;
}

function renderIncident(data, target) {
  const r = data.report;
  const a = r.attribution;
  const top = r.incident.largest_loss;
  const name = top ? `${top.method.toUpperCase()} · ${top.gateway}` : "No negative route contribution";
  const changes = r.incident.nearby_changes.map((change) => `${change.change_id} · ${dateText(change.deployed_at)}`);
  const stages = [["Started", "started"], ["Method selected within SLA", "selected"], ["Initiated within SLA", "initiated"],
    ["Success within SLA", "success_sla"], ["Failure within SLA", "fail_sla"], ["No terminal within SLA", "no_terminal_within_sla"]];
  target.innerHTML = `
    <div class="stats">
      ${stat("Success within SLA", percent(r.current.success_within_sla_pct), "Baseline " + percent(r.baseline.success_within_sla_pct))}
      ${stat("Change vs baseline", pp(r.delta_pp), "Mature cohorts of equal duration", color(r.delta_pp))}
      ${stat("Mature starts", integer(r.current.started), "Baseline " + integer(r.baseline.started))}
      ${stat("Still maturing", integer(r.recent_attempts), "Starts after the mature cutoff")}
    </div>
    <div class="grid">
      <section class="panel">
        <h2>Hourly success within SLA</h2>
        <p class="panel-description">${r.outcome_minutes}-minute outcome window · aligned by hours from each cohort start</p>
        <div class="chart-wrap"><canvas role="img" aria-label="Current and baseline hourly success rates. Exact points are in the hourly data table."></canvas></div>
        <div class="legend"><span><i></i>Current cohort</span><span><i></i>Baseline cohort</span></div>
        <details><summary>Hourly data</summary>${table(["Cohort", "UTC hour", "Starts", "Success within SLA"],
          r.hourly.map((b) => [b.cohort, dateText(b.hour + ":00:00Z"), integer(b.started), percent(b.success_within_sla_pct)]),
          [2, 3], "Hourly cohort data")}</details>
      </section>
      <section class="panel">
        <h2>What contributed to the change</h2>
        <div class="signal">
          <strong>${escapeHtml(name)}</strong>
          <small>${top ? escapeHtml(`${percent(top.baseline.success_within_sla_pct)} → ${percent(top.current.success_within_sla_pct)} · ${integer(top.current.started)} mature starts`)
            : "Compare route rates and traffic shares below."}</small>
        </div>
        <dl class="breakdown">
          ${[["Within-route performance", a?.within_pp], ["Routing mix", a?.mix_pp], ["Routes appearing or disappearing", a?.unmatched_pp]]
          .map(([label, value]) => `<div><dt>${label}</dt><dd class="${color(value)}">${pp(value)}</dd></div>`).join("")}
          <div><dt>Late terminals already observed</dt><dd>${integer(r.current.later)}</dd></div>
          <div><dt>Still unresolved at cutoff</dt><dd>${integer(r.current.unresolved_at_cutoff)}</dd></div>
          <div><dt>Gateway timeouts within SLA</dt><dd>${integer(r.current.timeout_sla)} · ${integer(r.baseline.timeout_sla)} prior</dd></div>
          <div><dt>Success among observed terminals</dt><dd>${percent(r.current.settled_only_success_pct)}</dd></div>
        </dl>
        <p class="panel-description">${r.incident.onset_hour_utc ? "First sustained drop around " +
          escapeHtml(dateText(r.incident.onset_hour_utc + ":00:00Z")) + "." : "No sustained hourly drop met the screening thresholds."}
          ${changes.length ? " Nearby change " + escapeHtml(changes.join(", ")) + " provides timing evidence." : ""}</p>
      </section>
    </div>
    <div class="grid equal">
      <section class="panel">
        <h2>Payment funnel</h2>
        <p class="panel-description">${escapeHtml(dateText(r.windows.current_start))} to ${escapeHtml(dateText(r.windows.mature_end))}</p>
        ${table(["Stage", "Baseline", "Current"], stages.map(([label, key]) =>
          [label, integer(r.baseline[key]), integer(r.current[key])]), [1, 2], "Payment funnel stages")}
        ${r.excluded_without_start ? `<p class="panel-description">${integer(r.excluded_without_start)} projections lacked a matching start event.</p>` : ""}
      </section>
      <section class="panel">
        <h2>Route contributions</h2>
        <p class="panel-description">Sorted by impact on overall success</p>
        ${r.segments.length ? table(["Route", "Previous SLA", "Current SLA", "Current share", "Impact"],
          r.segments.map((s) => [s.method.toUpperCase() + " · " + s.gateway, percent(s.baseline.success_within_sla_pct),
            percent(s.current.success_within_sla_pct), percent(s.current_share_pct, 1), pp(s.impact_pp)]),
          [1, 2, 3, 4], "Route contributions") : '<p class="empty">Both cohorts need eligible starts.</p>'}
      </section>
    </div>`;
  const canvas = target.querySelector("canvas");
  chartObserver?.disconnect();
  chartObserver = new ResizeObserver(() => drawChart(canvas, r));
  chartObserver.observe(canvas);
  drawChart(canvas, r);
}

function drawChart(canvas, report) {
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  if (width < 100 || height < 100) return;
  const ratio = Math.min(window.devicePixelRatio || 1, 2);
  canvas.width = Math.round(width * ratio);
  canvas.height = Math.round(height * ratio);
  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);
  const points = report.hourly;
  const rates = points.map((v) => v.success_within_sla_pct).filter((v) => v != null);
  const min = Math.max(0, Math.min(80, Math.floor(Math.min(...rates, 100) / 10) * 10));
  const left = 39, top = 10, plotWidth = width - 52, plotHeight = height - 33;
  const { baseline_start: baseline, current_start: current } = report.windows;
  const hours = (Date.parse(current) - Date.parse(baseline)) / 3600000;
  const x = (elapsed) => left + Math.max(0, elapsed) * plotWidth / hours;
  const y = (rate) => top + (100 - rate) * plotHeight / (100 - min);
  ctx.font = "10px ui-monospace, monospace";
  ctx.textBaseline = "middle";
  ctx.lineWidth = 1;
  for (let tick = 0; tick <= 4; tick++) {
    const rate = 100 - (100 - min) * tick / 4;
    const pos = y(rate);
    ctx.strokeStyle = "#30394e";
    ctx.beginPath(); ctx.moveTo(left, pos); ctx.lineTo(left + plotWidth, pos); ctx.stroke();
    ctx.fillStyle = "#9eaec9";
    ctx.fillText(Math.round(rate) + "%", 2, pos);
  }
  for (let tick = 0; tick <= 4; tick++) {
    const h = Math.round(hours * tick / 4);
    ctx.fillText("+" + h + "h", x(h) - (tick === 4 ? 21 : 0), height - 7);
  }
  for (const [period, start, stroke] of [["baseline", baseline, "#8595b4"], ["current", current, "#a1aaff"]]) {
    const series = points.filter((point) => point.cohort === period && point.success_within_sla_pct != null);
    let last = null;
    const markers = [];
    ctx.strokeStyle = stroke;
    ctx.lineWidth = period === "current" ? 2.3 : 1.45;
    ctx.lineJoin = "round";
    ctx.setLineDash(period === "baseline" ? [5, 4] : []);
    ctx.beginPath();
    for (const bucket of series) {
      const elapsed = (Date.parse(bucket.hour + ":00:00Z") - Date.parse(start)) / 3600000;
      const px = x(elapsed), py = y(bucket.success_within_sla_pct);
      if (last === null || elapsed - last > 1.1) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
      last = elapsed;
      markers.push([px, py]);
    }
    ctx.stroke();
    if (series.length < 30 && period === "current") {
      ctx.fillStyle = stroke;
      for (const [px, py] of markers) {
        ctx.beginPath(); ctx.arc(px, py, 2.4, 0, Math.PI * 2); ctx.fill();
      }
    }
  }
}

function renderExperiment(data, target) {
  const r = data.report;
  const label = verdicts[r.verdict] ?? "Readout";
  const state = { review_candidate: "pass", invalid: "fail", hold: "fail", inconclusive: "pending" }[r.verdict];
  const e = r.effect;
  const armRows = Object.entries(r.arms).map(([arm, counts]) => [arm, integer(counts.assigned), integer(counts.mature),
    integer(counts.attempted), integer(counts.mature - counts.attempted), integer(counts.converted)]);
  const gates = r.gates.map(({ name, state }) =>
    `<div><dt>${escapeHtml(name)}</dt><dd><span class="pill ${state}">${state}</span></dd></div>`).join("");
  target.innerHTML = `
    <div class="stats">
      ${stat("Decision", label, "Rollout requires human approval", state === "fail" ? "loss" : "")}
      ${stat("Assigned users", integer(Object.values(r.arms).reduce((sum, arm) => sum + arm.assigned, 0)), "Randomized user denominator")}
      ${stat("Treatment effect", pp(e?.delta_pp), e ? "95% CI " + e.ci95_pp.map(pp).join(" to ") : "Available at the fixed horizon", color(e?.delta_pp))}
      ${stat("Control → treatment", e ? percent(e.control_pct, 1) + " → " + percent(e.treatment_pct, 1) : "—", "Success per eligible assigned user")}
    </div>
    <div class="grid equal">
      <section class="panel">
        <h2>Assignment and outcome counts</h2>
        <p class="panel-description">Conversion counts a successful payment within ${r.protocol.outcome_hours} hours of assignment.</p>
        ${table(["Arm", "Assigned", "Mature", "Started", "No start", "Converted"], armRows, [1, 2, 3, 4, 5], "Assigned experiment users")}
        <p class="panel-description">Fixed readout available from ${escapeHtml(dateText(r.final_after))}. SRM p-value ${r.srm.p_value == null ? "—" : r.srm.p_value.toPrecision(3)}.</p>
      </section>
      <section class="panel">
        <h2>Decision gates <span class="pill ${state}">${escapeHtml(label)}</span></h2>
        <dl class="breakdown">${gates}</dl>
      </section>
    </div>
    <section class="panel">
      <h2>Guardrail margins</h2>
      <p class="panel-description">An upper confidence bound above the allowed increase keeps a guardrail from passing.</p>
      ${r.guardrails.length ? table(["Guardrail", "Control users", "Treatment users", "Risk difference interval", "Max increase", "Status"],
        r.guardrails.map((g) => [g.name.replaceAll("_", " "), integer(g.control_events), integer(g.treatment_events),
          g.ci_pp.map(pp).join(" to "), pp(g.margin_pp), g.state]), [1, 2, 3, 4], "Guardrail margins") : '<p class="empty">Guardrail readout is pending the experiment gates.</p>'}
    </section>`;
}

function renderEvidence() {
  const target = byId("evidence-content");
  if (!lastReport) {
    target.innerHTML = '<p class="empty">Run an incident or experiment to see its query evidence.</p>';
    return;
  }
  const info = lastReport;
  target.innerHTML = `<div class="stack">
    <section class="panel">
      <h2>${escapeHtml(info.source.title)}</h2>
      <p class="panel-description">${escapeHtml(sourceStatus(info))}</p>
      <p class="meta">Run ID ${escapeHtml(info.run_id)}</p>
      <p class="meta">Metric ${escapeHtml(info.report.metric.id)} · version ${info.report.metric.version}</p>
    </section>
    ${info.evidence.map((q) => `<section class="panel">
      <div class="measure"><h2>${escapeHtml(q.template)}</h2><span class="pill">${integer(q.rows)} rows · ${integer(q.duration_ms)} ms</span></div>
      <p class="meta">SQL SHA-256 ${escapeHtml(q.sha256)}</p>
      <details><summary>Bound parameters and SQL</summary><pre>${escapeHtml(JSON.stringify(q.params, null, 2))}</pre>
      <pre>${escapeHtml(q.sql)}</pre></details>
      </section>`).join("")}</div>`;
}

function sourceLink(label, url) {
  const link = new URL(url);
  if (link.protocol !== "https:") return "";
  return `<a class="source-link" href="${escapeHtml(link.href)}" target="_blank" rel="noopener">${escapeHtml(label)} ↗</a>`;
}

function renderBenchmarks(data, target) {
  const s = data.shoppers, u = data.upworthy, b = data.pix;
  target.innerHTML = `<div class="benchmark-grid">
    <section class="panel"><span class="source-badge public">Observed shopping sessions</span>
      <h2>UCI online shoppers</h2><p class="panel-description">Purchase conversion by visitor group.</p>
      <div class="measure"><strong>${percent(s.conversion_pct)}</strong><span class="meta">${integer(s.sessions)} sessions</span></div>
      ${table(["Visitor group", "Sessions", "Purchase"], s.groups.map((g) =>
      [g.group.replaceAll("_", " "), integer(g.sessions), percent(g.conversion_pct)]), [1, 2], "UCI shopper sessions")}
      ${sourceLink("UCI source · CC BY 4.0", s.url)}
    </section>
    <section class="panel"><span class="source-badge public">Randomized headline tests</span>
      <h2>Upworthy archive</h2>
      <p class="panel-description">${integer(u.evaluated_tests)} eligible tests · ${integer(u.excluded_risk)} excluded for documented randomization risk.</p>
      <label class="field" for="archive-test">Published test sample</label>
      <select id="archive-test">${u.samples.map((sample, i) => `<option value="${i}">Test ${escapeHtml(sample.id.slice(0, 10))}</option>`).join("")}</select>
      <div id="archive-readout"></div>
      ${sourceLink("Archive source · CC BY 4.0", u.url)}
    </section>
    ${b ? `<section class="panel"><span class="source-badge public">Published payment infrastructure</span>
      <h2>Banco Central do Brasil · Pix</h2><p class="panel-description">SPI availability index and the regulator’s monthly minimum.</p>
      ${table(["Month", "Availability", "Minimum"], b.months.slice(0, 6).map((m) => [m.month, percent(m.index_pct, 4), percent(m.floor_pct)]), [1, 2], "SPI availability")}
      <p class="panel-description">Network availability measures service uptime. It cannot measure individual checkout or authorization success.</p>
      ${sourceLink("BCB source · ODbL", b.url)}</section>` : ""}
  </div><p class="panel-description">Published datasets measure different outcomes. The replay scenarios use synthetic gateway events.</p>`;
  function selectArchive() {
    const selected = u.samples[Number(byId("archive-test").value)];
    byId("archive-readout").innerHTML = selected ?
      table(["Arm", "Impressions", "Clicks"], selected.arms.map((arm, i) => ["Arm " + (i + 1), integer(arm[0]), integer(arm[1])]),
        [1, 2], "Upworthy aggregate counts") +
      `<p class="panel-description">Screening estimate ${pp(selected.delta_pp)} · adjusted p ${escapeHtml(selected.adjusted_p)}
      · SRM ${selected.srm_ok ? "pass" : "flagged"}. Aggregate headline results are exploratory.</p>` :
      "<p>No benchmark tests passed input validation.</p>";
  }
  byId("archive-test").addEventListener("change", selectArchive);
  selectArchive();
}

async function load(view, path, options, render) {
  requests.get(view)?.abort();
  const controller = new AbortController();
  requests.set(view, controller);
  setStatus(view, "Calculating…");
  sources.delete(view);
  byId(view + "-content").replaceChildren();
  updateExport();
  let timer = setTimeout(() => controller.abort("timeout"), 25000);
  try {
    const data = await request(path, { ...options, signal: controller.signal });
    if (requests.get(view) !== controller) return;
    render(data, byId(view + "-content"));
    sources.set(view, data);
    if (data.run_id) {
      lastReport = data;
      setStatus(view, sourceStatus(data));
      renderEvidence();
    } else {
      setStatus(view, "Source counts and licensing verified at snapshot build");
    }
    updateExport();
  } catch (error) {
    if (requests.get(view) !== controller) return;
    setStatus(view, controller.signal.reason === "timeout" ? "Request timed out. Try again." :
      error.message || "Could not load this view", true);
  } finally {
    clearTimeout(timer);
  }
}

function incident() {
  const scope = new URLSearchParams(new FormData(byId("scope")));
  if (!scope.get("method")) scope.delete("method");
  return load("incident", "/api/v2/replay?" + scope.toString(), {}, renderIncident);
}

function experiment() {
  return load("experiment", "/api/v2/experiments/checkout?stage=" + byId("trial-stage").value, {}, renderExperiment);
}

function updateExport() {
  const data = active === "evidence" ? lastReport : sources.get(active);
  document.querySelectorAll("[data-export]").forEach((button) => { button.disabled = !data?.run_id; });
}

function showView() {
  const selected = location.hash.slice(1);
  active = ["incident", "experiment", "benchmarks", "evidence", "live"].includes(selected) ? selected : "incident";
  document.querySelectorAll(".view").forEach((view) => { view.hidden = view.id !== active; });
  document.querySelectorAll("[data-view]").forEach((link) => {
    if (link.dataset.view === active) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  });
  if (sources.has(active)) {
    const viewData = sources.get(active);
    const canvas = byId(active).querySelector("canvas");
    if (canvas) drawChart(canvas, viewData.report);
  } else {
    if (active === "incident") incident();
    if (active === "experiment") experiment();
    if (active === "benchmarks") load("benchmarks", "/api/v2/benchmarks", {}, renderBenchmarks);
  }
  if (active === "evidence") renderEvidence();
  updateExport();
}

byId("scope").addEventListener("change", incident);
byId("trial-stage").addEventListener("change", experiment);
byId("live-scope").elements.report.addEventListener("change", (event) => {
  byId("live-id").hidden = event.target.value === "incident";
});
byId("live-scope").addEventListener("submit", (event) => {
  event.preventDefault();
  const fields = new FormData(event.currentTarget);
  const token = fields.get("token");
  const kind = fields.get("report");
  const experimentId = fields.get("experiment_id").trim();
  if (kind === "experiment" && !experimentId) {
    setStatus("live", "Enter the registered experiment ID.", true);
    return;
  }
  const path = kind === "experiment" ? "/api/v2/live/experiments/" + encodeURIComponent(experimentId) : "/api/v2/live/replay";
  load("live", path, { method: "POST", headers: { Authorization: "Bearer " + token, "Content-Type": "application/json" },
    body: kind === "incident" ? "{}" : undefined }, kind === "experiment" ? renderExperiment : renderIncident);
});
document.querySelectorAll("[data-export]").forEach((button) => button.addEventListener("click", () => {
  const data = active === "evidence" ? lastReport : sources.get(active);
  if (!data?.run_id) return;
  const url = URL.createObjectURL(new Blob([JSON.stringify(data, null, 2)], { type: "application/json" }));
  const link = document.createElement("a");
  link.href = url;
  link.download = "paylens-" + data.run_id + ".json";
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}));
request("/api/v2/health").then(() => {
  const status = byId("connection");
  status.classList.add("online");
  status.lastChild.textContent = "API connected";
}).catch(() => {
  const status = byId("connection");
  status.classList.add("offline");
  status.lastChild.textContent = "API unavailable";
});
window.addEventListener("hashchange", showView);
showView();
