let trendChartInstance = null;
let topChartInstance = null;
let tableInstance = null;
let pendingTablePlaceholder = "加载中...";
let currentRows = [];
let lastSummary = null;
let lastCharts = null;
let tableMetrics = null;
let echartsReadyPromise = null;
let tabulatorReadyPromise = null;

function ensureEchartsLoaded() {
  if (typeof window.echarts !== "undefined") {
    return Promise.resolve(window.echarts);
  }
  if (!echartsReadyPromise) {
    echartsReadyPromise = new Promise(function wait(resolve, reject) {
      let attempts = 0;
      var timer = setInterval(function check() {
        if (typeof window.echarts !== "undefined") {
          clearInterval(timer);
          resolve(window.echarts);
          return;
        }
        attempts += 1;
        if (attempts > 50) {
          clearInterval(timer);
          reject(new Error("ECharts 脚本加载失败"));
        }
      }, 200);
    }).catch(function reset(err) {
      echartsReadyPromise = null;
      throw err;
    });
  }
  return echartsReadyPromise;
}

function $(id) {
  return document.getElementById(id);
}

function fmtAmount(value, digits) {
  const v = Number(value);
  const precision = typeof digits === "number" ? digits : 2;
  if (isNaN(v)) return "--";
  const abs = Math.abs(v);
  if (abs >= 1e8) return (v / 1e8).toFixed(precision) + " 亿";
  if (abs >= 1e4) return (v / 1e4).toFixed(precision) + " 万";
  return v.toFixed(precision);
}

function fmtNumber(value, digits) {
  const v = Number(value);
  if (isNaN(v)) return "--";
  const p = typeof digits === "number" ? digits : 2;
  return v.toFixed(p);
}

function fmtPercent(value) {
  const v = Number(value);
  if (isNaN(v)) return "--";
  return v.toFixed(1) + "%";
}

function parseDateString(value) {
  if (!value) return null;
  const parts = value.split("-");
  if (parts.length !== 3) return null;
  const year = Number(parts[0]);
  const month = Number(parts[1]) - 1;
  const day = Number(parts[2]);
  if (
    Number.isNaN(year) ||
    Number.isNaN(month) ||
    Number.isNaN(day) ||
    month < 0 ||
    month > 11 ||
    day < 1 ||
    day > 31
  ) {
    return null;
  }
  return new Date(year, month, day);
}

function formatDateInput(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return "";
  }
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return y + "-" + m + "-" + d;
}

function axisValueFormatter(value) {
  const v = Number(value);
  if (isNaN(v)) return "";
  if (Math.abs(v) >= 1e8) return (v / 1e8).toFixed(1) + "亿";
  if (Math.abs(v) >= 1e4) return (v / 1e4).toFixed(1) + "万";
  return v.toFixed(0);
}

function getChartColors() {
  const styles = getComputedStyle(document.body);
  function read(name, fallback) {
    const value = styles.getPropertyValue(name);
    return value && value.trim() ? value.trim() : fallback;
  }
  return {
    axisLine: read("--chart-axis-color", "#475569"),
    axisLabel: read("--chart-axis-label", "#cbd5f5"),
    gridLine: read("--chart-grid-line", "rgba(148, 163, 184, 0.18)"),
    line: read("--chart-line-color", "#38bdf8"),
    area: read("--chart-area-color", "rgba(56, 189, 248, 0.18)"),
    sliderBorder: read("--chart-slider-border", "rgba(148, 163, 184, 0.3)"),
    sliderFill: read("--chart-slider-fill", "rgba(56, 189, 248, 0.2)"),
    titleText: read("--chart-title-text", "#94a3b8"),
    emptyText: read("--chart-empty-text", "#64748b"),
    barStart: read("--chart-bar-start", "rgba(99, 102, 241, 0.85)"),
    barEnd: read("--chart-bar-end", "rgba(37, 99, 235, 0.65)"),
    barSliderFill: read("--chart-bar-slider-fill", "rgba(99, 102, 241, 0.2)")
  };
}

function clampPercent(value) {
  const v = Number(value);
  if (isNaN(v)) return 0;
  if (v < 0) return 0;
  if (v > 100) return 100;
  return v;
}

function getTrendChart() {
  if (!trendChartInstance) {
    const el = $("trendChart");
    if (el && typeof window.echarts !== "undefined") {
      trendChartInstance = echarts.init(el);
    }
  }
  return trendChartInstance;
}

function getTopChart() {
  if (!topChartInstance) {
    const el = $("chart");
    if (el && typeof window.echarts !== "undefined") {
      topChartInstance = echarts.init(el);
    }
  }
  return topChartInstance;
}

function getTabulatorClass() {
  if (typeof window === "undefined") return null;
  if (typeof window.Tabulator !== "undefined") return window.Tabulator;
  if (typeof window.TabulatorFull !== "undefined") return window.TabulatorFull;
  return null;
}

function ensureTabulatorReady() {
  const Tab = getTabulatorClass();
  if (Tab) {
    return Promise.resolve(Tab);
  }
  if (typeof window !== "undefined" && typeof window.__loadLocalTabulatorAssets === "function") {
    window.__loadLocalTabulatorAssets();
  }
  if (!tabulatorReadyPromise) {
    tabulatorReadyPromise = new Promise(function wait(resolve, reject) {
      let attempts = 0;
      const timer = setInterval(function check() {
        const ready = getTabulatorClass();
        if (ready) {
          clearInterval(timer);
          resolve(ready);
          return;
        }
        attempts += 1;
        if (attempts > 60) {
          clearInterval(timer);
          reject(new Error("表格组件加载失败"));
        }
      }, 200);
    }).catch(function reset(err) {
      tabulatorReadyPromise = null;
      throw err;
    });
  }
  return tabulatorReadyPromise;
}

function escapeHtml(value) {
  if (typeof value !== "string") {
    return value;
  }
  return value.replace(/[&<>"']/g, function replace(char) {
    switch (char) {
      case "&": return "&amp;";
      case "<": return "&lt;";
      case ">": return "&gt;";
      case '"': return "&quot;";
      case "'": return "&#39;";
      default: return char;
    }
  });
}

function percentOf(value, maxValue) {
  const numeric = Number(value);
  const max = Number(maxValue);
  if (!Number.isFinite(numeric) || numeric <= 0 || !Number.isFinite(max) || max <= 0) {
    return 0;
  }
  const pct = (numeric / max) * 100;
  return pct > 100 ? 100 : pct;
}

function resolvePlanDisplay(row) {
  const planKey = typeof row.plan_key === "string" ? row.plan_key : "";
  const planLabel = typeof row.plan_label === "string" ? row.plan_label : "";
  const isLegacyPlan = planKey.startsWith("__DEFAULT__:");
  let planText = "--";
  if (planLabel) {
    planText = planLabel;
  } else if (planKey && !isLegacyPlan) {
    planText = planKey.toUpperCase();
  } else if (planKey && isLegacyPlan) {
    planText = "默认计划";
  }

  let planKeyDisplay = "";
  if (planKey) {
    if (isLegacyPlan) {
      const idx = planKey.indexOf(":");
      planKeyDisplay = idx >= 0 ? planKey.slice(idx + 1) : "";
    } else {
      planKeyDisplay = planKey.toUpperCase();
    }
  }

  const planMeta = [];
  if (row.plan_progress_text) planMeta.push("进度 " + row.plan_progress_text);
  if (row.plan_announce_date) planMeta.push("公告 " + row.plan_announce_date);
  if (row.plan_start_date) planMeta.push("生效 " + row.plan_start_date);

  const priceLower = Number(row.plan_price_lower);
  const priceUpper = Number(row.plan_price_upper);
  const hasLower = Number.isFinite(priceLower);
  const hasUpper = Number.isFinite(priceUpper);
  if (hasLower && hasUpper) {
    if (priceLower === priceUpper) {
      planMeta.push("价格 ≤ " + fmtNumber(priceUpper, 2) + " 元");
    } else {
      planMeta.push("价格 " + fmtNumber(priceLower, 2) + "-" + fmtNumber(priceUpper, 2) + " 元");
    }
  } else if (hasUpper) {
    planMeta.push("价格 ≤ " + fmtNumber(priceUpper, 2) + " 元");
  } else if (hasLower) {
    planMeta.push("价格 ≥ " + fmtNumber(priceLower, 2) + " 元");
  }

  const planAmount = Number(row.plan_amount_upper);
  if (Number.isFinite(planAmount) && planAmount > 0) {
    planMeta.push("金额 ≤ " + fmtAmount(planAmount));
  }
  const planVolume = Number(row.plan_volume_upper);
  if (Number.isFinite(planVolume) && planVolume > 0) {
    planMeta.push("数量 ≤ " + fmtNumber(planVolume, 0));
  }
  const planLatestPrice = Number(row.plan_latest_price);
  if (Number.isFinite(planLatestPrice) && planLatestPrice > 0) {
    planMeta.push("最新价 " + fmtNumber(planLatestPrice, 2) + " 元");
  }

  if (row.start_date) planMeta.push("记录开始 " + row.start_date);
  if (row.end_date) planMeta.push("记录结束 " + row.end_date);

  return {
    planText: planText,
    planKeyDisplay: planKeyDisplay,
    planMeta: planMeta,
    isLegacyPlan: isLegacyPlan
  };
}

function renderMetricCell(value, maxValue, suffix) {
  const pct = percentOf(value, maxValue);
  const pieces = [];
  if (pct > 0) {
    pieces.push(fmtPercent(pct));
  }
  if (suffix) {
    pieces.push(suffix);
  }
  const secondary = pieces.length ? pieces.join(" · ") : "--";
  return [
    '<div class="cell-metric">',
    '  <div class="metric-primary">' + fmtAmount(value) + "</div>",
    '  <div class="data-bar">',
    '    <div class="data-bar-track">',
    '      <div class="data-bar-fill" style="width:' + pct.toFixed(1) + '%"></div>',
    "    </div>",
    '    <span class="metric-secondary">' + secondary + "</span>",
    "  </div>",
    "</div>"
  ].join("");
}

function renderAvgPriceCell(value) {
  return [
    '<div class="cell-wrap">',
    '  <span class="metric-primary">' + fmtNumber(value, 2) + "</span>",
    '  <span class="metric-secondary">元/股</span>',
    "</div>"
  ].join("");
}

function renderProgressCell(pctValue, text) {
  const pct = clampPercent(pctValue);
  const extra = text ? " · " + escapeHtml(text) : "";
  return [
    '<div class="progress">',
    '  <div class="progress-track">',
    '    <div class="progress-fill" style="width:' + pct + '%"></div>',
    "  </div>",
    '  <div class="muted">' + fmtPercent(pct) + extra + "</div>",
    "</div>"
  ].join("");
}

function codeFormatter(cell) {
  const value = cell.getValue();
  const code = value ? escapeHtml(String(value)) : "--";
  return '<span class="badge badge--code" title="股票代码">' + code + "</span>";
}

function nameFormatter(cell) {
  const value = cell.getValue();
  const name = value ? escapeHtml(String(value)) : "--";
  return '<div class="cell-wrap"><span class="metric-primary">' + name + "</span></div>";
}

function planFormatter(cell) {
  const data = cell.getData();
  const planInfo = resolvePlanDisplay(data);
  const badges = [];
  const planText = planInfo.planText && planInfo.planText !== "--" ? escapeHtml(planInfo.planText) : "";
  const planKeyDisplay = planInfo.planKeyDisplay ? escapeHtml(planInfo.planKeyDisplay) : "";
  if (planText && planText === planKeyDisplay) {
    badges.push('<span class="badge badge--plan" title="计划">' + planText + "</span>");
  } else {
    if (planText) {
      badges.push('<span class="badge badge--plan" title="计划名称">' + planText + "</span>");
    }
    if (planKeyDisplay) {
      badges.push('<span class="badge badge--plan-code" title="计划标识">' + planKeyDisplay + "</span>");
    }
  }
  if (!badges.length) {
    badges.push('<span class="metric-secondary">--</span>');
  }
  const meta = planInfo.planMeta.length
    ? '<span class="metric-secondary">' + planInfo.planMeta.map(function mapMeta(text) {
      return escapeHtml(text);
    }).join(" · ") + "</span>"
    : "";
  return [
    '<div class="cell-wrap">',
    '  <div class="plan-badge-row">' + badges.join("") + "</div>",
    meta ? "  " + meta : "",
    "</div>"
  ].join("");
}

function dateFormatter(cell) {
  const data = cell.getData();
  const date = data.date ? escapeHtml(String(data.date)) : "--";
  const isLatest = tableMetrics && tableMetrics.latestDate && data.date === tableMetrics.latestDate;
  const badge = isLatest ? '<span class="badge badge--latest">最新披露</span>' : "";
  return [
    '<div class="cell-wrap">',
    '  <span class="metric-primary">' + date + "</span>",
    badge ? "  " + badge : "",
    "</div>"
  ].join("");
}

function amountFormatter(cell) {
  const data = cell.getData();
  const max = tableMetrics ? tableMetrics.amountMax : 0;
  return renderMetricCell(data.amount, max, "占表内最高");
}

function cumAmountFormatter(cell) {
  const data = cell.getData();
  const max = tableMetrics ? tableMetrics.cumAmountMax : 0;
  return renderMetricCell(data.cumulative_amount, max, "累计占表内最高");
}

function volumeFormatter(cell) {
  const data = cell.getData();
  const max = tableMetrics ? tableMetrics.volumeMax : 0;
  return renderMetricCell(data.volume, max, "数量占表内最高");
}

function cumVolumeFormatter(cell) {
  const data = cell.getData();
  const max = tableMetrics ? tableMetrics.cumVolumeMax : 0;
  return renderMetricCell(data.cumulative_volume, max, "累计数量占表内最高");
}

function avgPriceFormatter(cell) {
  const data = cell.getData();
  return renderAvgPriceCell(data.avg_price);
}

function progressFormatter(cell) {
  const data = cell.getData();
  return renderProgressCell(data.progress_pct, data.progress_text);
}

function buildTableColumns() {
  return [
    {
      title: "代码",
      field: "code",
      width: 120,
      minWidth: 120,
      sorter: "string",
      headerTooltip: "点击排序",
      formatter: codeFormatter
    },
    {
      title: "名称",
      field: "name",
      minWidth: 180,
      sorter: "string",
      headerTooltip: "点击排序",
      formatter: nameFormatter
    },
    {
      title: "计划代码",
      field: "plan_label",
      minWidth: 240,
      sorter: "string",
      headerTooltip: "计划名称 + 编号",
      formatter: planFormatter
    },
    {
      title: "日期",
      field: "date",
      width: 140,
      minWidth: 140,
      sorter: "string",
      headerTooltip: "点击排序",
      formatter: dateFormatter
    },
    {
      title: "当日金额",
      field: "amount",
      minWidth: 200,
      sorter: "number",
      headerTooltip: "点击排序",
      formatter: amountFormatter
    },
    {
      title: "累计金额",
      field: "cumulative_amount",
      minWidth: 220,
      sorter: "number",
      headerTooltip: "点击排序",
      formatter: cumAmountFormatter
    },
    {
      title: "当日数量",
      field: "volume",
      minWidth: 200,
      sorter: "number",
      headerTooltip: "点击排序",
      formatter: volumeFormatter
    },
    {
      title: "累计数量",
      field: "cumulative_volume",
      minWidth: 220,
      sorter: "number",
      headerTooltip: "点击排序",
      formatter: cumVolumeFormatter
    },
    {
      title: "均价",
      field: "avg_price",
      width: 140,
      minWidth: 140,
      sorter: "number",
      headerTooltip: "点击排序",
      formatter: avgPriceFormatter
    },
    {
      title: "累计进度",
      field: "progress_pct",
      minWidth: 220,
      sorter: "number",
      headerTooltip: "计划完成度",
      formatter: progressFormatter
    }
  ];
}

function setTablePlaceholder(message) {
  pendingTablePlaceholder = message || "";
  if (tableInstance && typeof tableInstance.updateOptions === "function") {
    tableInstance.updateOptions({ placeholder: pendingTablePlaceholder });
  }
}

function ensureTable() {
  if (tableInstance) {
    return tableInstance;
  }
  const container = $("tbl");
  if (!container) {
    return null;
  }
  const Tab = getTabulatorClass();
  if (!Tab) {
    return null;
  }
  tableInstance = new Tab(container, {
    layout: "fitDataStretch",
    height: "520px",
    reactiveData: false,
    selectable: false,
    placeholder: pendingTablePlaceholder,
    rowHeight: 76,
    columnDefaults: {
      headerHozAlign: "left",
      hozAlign: "left",
      resizable: true
    },
    initialSort: [
      { column: "date", dir: "desc" },
      { column: "amount", dir: "desc" }
    ],
    columns: buildTableColumns(),
    rowFormatter: function highlightLatest(row) {
      const element = row.getElement();
      if (!element) return;
      element.classList.remove("tabulator-row--latest");
      const data = row.getData();
      if (tableMetrics && tableMetrics.latestDate && data.date === tableMetrics.latestDate) {
        element.classList.add("tabulator-row--latest");
      }
    }
  });
  if (pendingTablePlaceholder) {
    setTablePlaceholder(pendingTablePlaceholder);
  }
  return tableInstance;
}

function buildParams() {
  const dateFromInput = $("date_from");
  const dateToInput = $("date_to");
  const limitInput = $("limit");
  const codeInput = $("code");

  const params = new URLSearchParams();
  const dateFrom = dateFromInput && dateFromInput.value ? dateFromInput.value : dateFromInput.dataset.default;
  if (dateFrom) params.set("date_from", dateFrom);
  const dateTo = dateToInput && dateToInput.value ? dateToInput.value : "";
  if (dateTo) params.set("date_to", dateTo);
  const codeVal = codeInput ? codeInput.value.trim() : "";
  if (codeVal) params.set("code", codeVal);
  if (limitInput && limitInput.value) params.set("limit", limitInput.value);
  return params.toString();
}

async function fetchDashboard() {
  const qs = buildParams();
  const resp = await fetch("/api/dashboard?" + qs);
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || "加载仪表盘数据失败");
  }
  return resp.json();
}

function updateSummary(summary) {
  $("statAmount").innerText = fmtAmount(summary.total_amount);
  $("statRange").innerText = summary.date_from + " ~ " + (summary.date_to || "今日");
  const companies = typeof summary.unique_codes === "number" ? summary.unique_codes : 0;
  const plans = typeof summary.unique_plans === "number" ? summary.unique_plans : null;
  let coverageText = companies ? String(companies) : "0";
  if (plans !== null) {
    coverageText = companies + " 家 · " + plans + " 个计划";
  }
  $("statCodes").innerText = coverageText;
  $("statDaily").innerText = fmtAmount(summary.avg_daily_amount);
  $("statLatest").innerText = summary.latest_date || "--";
}

function renderTrend(trend) {
  if (typeof window.echarts === "undefined") return;
  const chart = getTrendChart();
  if (!chart) return;

  const hasData = Array.isArray(trend.dates) && trend.dates.length > 0;
  const rotate = hasData && trend.dates.length > 12 ? 45 : 0;
  const colors = getChartColors();
  const option = {
    backgroundColor: "transparent",
    tooltip: { trigger: "axis" },
    grid: { top: 50, left: 60, right: 36, bottom: 90 },
    xAxis: {
      type: "category",
      data: hasData ? trend.dates : [],
      boundaryGap: false,
      axisLine: { lineStyle: { color: colors.axisLine } },
      axisLabel: { color: colors.axisLabel, rotate: rotate }
    },
    yAxis: {
      type: "value",
      axisLine: { show: false },
      splitLine: { lineStyle: { color: colors.gridLine } },
      axisLabel: {
        color: colors.axisLabel,
        formatter: function format(value) {
          return axisValueFormatter(value);
        }
      }
    },
    series: [{
      type: "line",
      smooth: true,
      data: hasData ? trend.amounts : [],
      lineStyle: { color: colors.line, width: 2 },
      areaStyle: { color: colors.area },
      showSymbol: false
    }],
    dataZoom: hasData ? [
      { type: "inside", start: 0, end: 100, zoomOnMouseWheel: true },
      { type: "slider", start: 0, end: 100, height: 16, bottom: 18, handleSize: 14, borderColor: colors.sliderBorder, fillerColor: colors.sliderFill }
    ] : [],
    graphic: hasData ? [] : [{
      type: "text",
      left: "center",
      top: "middle",
      style: { text: "暂无趋势数据", fill: colors.emptyText, fontSize: 16 }
    }]
  };
  chart.setOption(option, true);
}

function renderTop(top) {
  if (typeof window.echarts === "undefined") return;
  const chart = getTopChart();
  if (!chart) return;
  const hasData = Array.isArray(top.labels) && top.labels.length > 0;
  const rotate = hasData && top.labels.length > 10 ? 45 : 0;
  const colors = getChartColors();
  const option = {
    backgroundColor: "transparent",
    tooltip: { trigger: "axis" },
    grid: { top: 60, left: 70, right: 36, bottom: 100 },
    xAxis: {
      type: "category",
      data: hasData ? top.labels : [],
      axisLabel: { color: colors.axisLabel, rotate: rotate },
      axisLine: { lineStyle: { color: colors.axisLine } }
    },
    yAxis: {
      type: "value",
      axisLabel: {
        color: colors.axisLabel,
        formatter: function format(value) {
          return axisValueFormatter(value);
        }
      },
      splitLine: { lineStyle: { color: colors.gridLine } }
    },
    series: [{
      type: "bar",
      data: hasData ? top.values : [],
      itemStyle: {
        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: colors.barStart },
          { offset: 1, color: colors.barEnd }
        ])
      },
      barMaxWidth: 38
    }],
    title: {
      text: top.date ? "披露日：" + top.date : "披露日：暂无数据",
      left: "center",
      top: 4,
      textStyle: { color: colors.titleText, fontSize: 12 }
    },
    graphic: hasData ? [] : [{
      type: "text",
      left: "center",
      top: "middle",
      style: { text: "暂无当日数据", fill: colors.emptyText, fontSize: 16 }
    }],
    dataZoom: hasData && top.labels.length > 10 ? [
      { type: "inside", start: 0, end: 100, zoomOnMouseWheel: true },
      { type: "slider", start: 0, end: 100, height: 16, bottom: 24, handleSize: 14, borderColor: colors.sliderBorder, fillerColor: colors.barSliderFill }
    ] : []
  };
  chart.setOption(option, true);
}

function renderTable(rows, summary) {
  const table = ensureTable();
  if (!table) {
    $("summary").innerText = "表格组件加载中...";
    ensureTabulatorReady().then(function onReady() {
      renderTable(rows, summary);
    }).catch(function onErr(err) {
      $("summary").innerText = err.message || "表格组件加载失败";
      console.error(err);
    });
    return;
  }

  if (!Array.isArray(rows) || rows.length === 0) {
    tableMetrics = null;
    setTablePlaceholder("选定区间内没有匹配数据");
    table.clearData();
    table.redraw(true);
    $("summary").innerText = "暂无数据";
    return;
  }

  const amountMax = rows.reduce(function maxAmount(acc, row) {
    const value = Number(row.amount);
    return Number.isFinite(value) && value > acc ? value : acc;
  }, 0);
  const cumAmountMax = rows.reduce(function maxCumAmount(acc, row) {
    const value = Number(row.cumulative_amount);
    return Number.isFinite(value) && value > acc ? value : acc;
  }, 0);
  const volumeMax = rows.reduce(function maxVolume(acc, row) {
    const value = Number(row.volume);
    return Number.isFinite(value) && value > acc ? value : acc;
  }, 0);
  const cumVolumeMax = rows.reduce(function maxCumVolume(acc, row) {
    const value = Number(row.cumulative_volume);
    return Number.isFinite(value) && value > acc ? value : acc;
  }, 0);

  tableMetrics = {
    amountMax: amountMax,
    cumAmountMax: cumAmountMax,
    volumeMax: volumeMax,
    cumVolumeMax: cumVolumeMax,
    latestDate: summary && summary.latest_date ? summary.latest_date : null
  };

  const preparedRows = rows.map(function clone(row) {
    return Object.assign({}, row);
  });

  table.setData(preparedRows).then(function onSet() {
    table.redraw(true);
  }).catch(function handleTableError(err) {
    console.error("表格数据加载失败", err);
  });

  const totalLabel = summary
    ? "当前展示 " + rows.length + " 条 · 区间累计金额 " + fmtAmount(summary.total_amount) + " · 覆盖计划 " + (summary.unique_plans || 0)
    : "当前展示 " + rows.length + " 条";
  $("summary").innerText = totalLabel;
}

function setActiveRange(button) {
  const container = document.querySelector(".range-btns");
  if (!container) return;
  const buttons = container.querySelectorAll("button");
  for (let i = 0; i < buttons.length; i += 1) {
    buttons[i].classList.remove("active");
  }
  if (button) button.classList.add("active");
}

function applyQuickRange(days) {
  const dateFrom = $("date_from");
  const dateTo = $("date_to");
  if (!dateFrom || !dateTo) return;
  const maxStr = dateFrom.dataset.max || dateTo.dataset.max || dateTo.value;
  const minStr = dateFrom.dataset.min || dateTo.dataset.min || null;
  const baseStr = dateTo.value && dateTo.value.length === 10 ? dateTo.value : maxStr;
  const maxDate = parseDateString(baseStr);
  const minDate = parseDateString(minStr);
  if (!maxDate) {
    return;
  }
  const target = new Date(maxDate);
  target.setDate(target.getDate() - (days - 1));
  if (minDate && target < minDate) {
    target.setTime(minDate.getTime());
  }
  dateFrom.value = formatDateInput(target);
  dateTo.value = formatDateInput(maxDate);
}

async function renderDashboard(triggerButton) {
  if (triggerButton) setActiveRange(triggerButton);
  try {
    $("summary").innerText = "加载中...";
    setTablePlaceholder("加载中...");
    const data = await fetchDashboard();
    currentRows = Array.isArray(data.table) ? data.table.slice() : [];
    lastSummary = data.summary;
    const charts = data.charts || {};
    lastCharts = charts;
    updateSummary(data.summary);
    if (typeof window.echarts !== "undefined") {
      renderTrend(charts.trend || { dates: [], amounts: [] });
      renderTop(charts.top || { labels: [], values: [] });
    } else {
      ensureEchartsLoaded().then(function onReady() {
        if (lastCharts) {
          renderTrend(lastCharts.trend || { dates: [], amounts: [] });
          renderTop(lastCharts.top || { labels: [], values: [] });
        }
      }).catch(function onFail(err) {
        console.warn(err);
      });
    }
    renderTable(currentRows, data.summary);
  } catch (err) {
    const message = err && err.message ? err.message : "加载失败";
    setTablePlaceholder(message);
    $("summary").innerText = message;
    console.error(err);
  }
}

function resetFilters() {
  const dateFrom = $("date_from");
  const dateTo = $("date_to");
  const code = $("code");
  const limit = $("limit");
  if (dateFrom) dateFrom.value = dateFrom.dataset.default || dateFrom.dataset.min;
  if (dateTo) dateTo.value = dateFrom ? dateFrom.dataset.max : dateTo.value;
  if (code) code.value = "";
  if (limit) limit.value = "1000";
  const defaultBtn = document.querySelector('.range-btns button[data-range="30"]');
  if (defaultBtn) {
    applyQuickRange(30);
    setActiveRange(defaultBtn);
  }
}


window.updateChartsTheme = function updateChartsTheme() {
  if (lastCharts) {
    renderTrend(lastCharts.trend || { dates: [], amounts: [] });
    renderTop(lastCharts.top || { labels: [], values: [] });
  }
};

window.updateTableTheme = function updateTableTheme() {
  if (tableInstance) {
    tableInstance.redraw(true);
  }
};

document.addEventListener("DOMContentLoaded", function registerEvents() {
  const btnLoad = $("btnLoad");
  const btnReset = $("btnReset");
  if (btnLoad) {
    btnLoad.addEventListener("click", function handleLoad() {
      renderDashboard(null);
    });
  }
  if (btnReset) {
    btnReset.addEventListener("click", function handleReset() {
      resetFilters();
      renderDashboard(null);
    });
  }


  const rangeContainer = document.querySelector(".range-btns");
  if (rangeContainer) {
    rangeContainer.addEventListener("click", function onRangeClick(event) {
      const target = event.target;
      if (target && target.tagName === "BUTTON" && target.dataset.range) {
        const days = parseInt(target.dataset.range, 10);
        if (!isNaN(days)) {
          applyQuickRange(days);
          renderDashboard(target);
        }
      }
    });
  }

  const dateTo = $("date_to");
  if (dateTo && !dateTo.value) {
    dateTo.value = dateTo.getAttribute("max");
  }

  const explicitDefaultBtn = document.querySelector('.range-btns button[data-range="30"]');
  if (explicitDefaultBtn) {
    applyQuickRange(30);
    setActiveRange(explicitDefaultBtn);
  } else {
    const defaultRangeBtn = document.querySelector('.range-btns button.active');
    if (defaultRangeBtn && defaultRangeBtn.dataset.range) {
      const defaultDays = parseInt(defaultRangeBtn.dataset.range, 10);
      if (!isNaN(defaultDays)) {
        applyQuickRange(defaultDays);
        setActiveRange(defaultRangeBtn);
      }
    }
  }

  window.addEventListener("resize", function onResize() {
    if (trendChartInstance) trendChartInstance.resize();
    if (topChartInstance) topChartInstance.resize();
    if (tableInstance) tableInstance.redraw(true);
  });

  const defaultBtn = explicitDefaultBtn || document.querySelector('.range-btns button.active');
  renderDashboard(defaultBtn || null);

  ensureTabulatorReady().catch(function onTableErr(err) {
    console.warn(err);
  });

  ensureEchartsLoaded().then(function afterLoad() {
    if (lastCharts) {
      renderTrend(lastCharts.trend || { dates: [], amounts: [] });
      renderTop(lastCharts.top || { labels: [], values: [] });
    }
  }).catch(function handleError(err) {
    console.error(err);
  });
});
