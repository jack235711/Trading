/**
 * FX Lab - Premium Analysis Platform (v33 - Complete Edition)
 * All features integrated: state persistence, chart sync, clean UI
 */

const API_BASE = (window.location.protocol === 'file:') ? 'http://127.0.0.1:8000' : '';

// --- State Persistence ---
const STATE_KEY = 'fxlab_state';
function saveState() {
    const state = {
        panes: window.panes.map(p => ({
            symbol: p.symbol,
            tf: p.tf,
            anchor: p.anchor.toISOString().split('T')[0],
            isSync: p.isSync
        })),
        prefs: {
            sma1: document.getElementById('check-sma20')?.checked,
            sma1_val: document.getElementById('param-sma20')?.value,
            sma2: document.getElementById('check-sma50')?.checked,
            sma2_val: document.getElementById('param-sma50')?.value,
            bb: document.getElementById('check-bb')?.checked,
            bb_p: document.getElementById('param-bb-period')?.value,
            bb_d: document.getElementById('param-bb-dev')?.value,
            rsi: document.getElementById('check-rsi')?.checked,
            rsi_val: document.getElementById('param-rsi')?.value,
            macd: document.getElementById('check-macd')?.checked,
            macd_f: document.getElementById('param-macd-fast')?.value,
            macd_s: document.getElementById('param-macd-slow')?.value,
            macd_sig: document.getElementById('param-macd-signal')?.value,
            macd2: document.getElementById('check-macd2')?.checked,
            macd2_f: document.getElementById('param-macd2-fast')?.value,
            macd2_s: document.getElementById('param-macd2-slow')?.value,
            macd2_sig: document.getElementById('param-macd2-signal')?.value,
            dow: document.getElementById('check-dow')?.checked,
            dow_p: document.getElementById('param-dow')?.value
        }
    };
    localStorage.setItem(STATE_KEY, JSON.stringify(state));
}

function loadState() {
    try {
        const saved = localStorage.getItem(STATE_KEY);
        return saved ? JSON.parse(saved) : null;
    } catch (e) { return null; }
}

// --- Connectivity Banner ---
function showConnBanner(msg) {
    let b = document.getElementById('conn-banner');
    if (!b) {
        b = document.createElement('div');
        b.id = 'conn-banner';
        b.style = 'position:fixed;top:0;left:72px;right:0;z-index:9999;padding:12px 24px;background:rgba(239,68,68,0.9);color:white;font-size:13px;font-weight:600;';
        document.body.appendChild(b);
    }
    b.textContent = '⚠️ ' + msg;
    b.style.display = 'block';
}

function hideConnBanner() {
    const b = document.getElementById('conn-banner');
    if (b) b.style.display = 'none';
}

// --- Market Dashboard ---
class MarketOverview {
    constructor() {
        this.container = document.getElementById('overview-grid');
        this.symbols = ['EURUSD', 'USDJPY', 'GBPUSD', 'EURJPY', 'EURGBP'];
        this.timeframes = [1, 5, 15, 60, 240];
        if (this.container) this.refreshAll();
    }

    async refreshAll() {
        this.container.innerHTML = '';
        for (const symbol of this.symbols) {
            const card = document.createElement('div');
            card.className = 'market-card';
            card.id = `card-${symbol}`;
            card.innerHTML = `<div class="card-symbol">${symbol}</div><div class="card-price">Loading...</div>`;
            this.container.appendChild(card);

            try {
                const res = await fetch(`${API_BASE}/ohlc?symbol=${symbol}&start_date=2025-03-25&end_date=2025-04-03`);
                if (!res.ok) throw new Error('HTTP ' + res.status);
                const data = await res.json();
                if (data?.length) {
                    const fmt = data.map(i => ({
                        time: Math.floor(new Date(i.time).getTime() / 1000),
                        open: i.open, high: i.high, low: i.low, close: i.close
                    })).filter(d => !isNaN(d.time)).sort((a, b) => a.time - b.time);
                    this.updateCard(symbol, fmt);
                    hideConnBanner();
                }
            } catch (e) { showConnBanner('Backend Error: ' + e.message); }
        }
    }

    updateCard(symbol, data) {
        const card = document.getElementById(`card-${symbol}`);
        if (!card || !data.length) return;
        const last = data[data.length - 1];
        const color = last.close >= (data[data.length - 2]?.close || 0) ? 'var(--price-up)' : 'var(--price-down)';

        let trends = '';
        this.timeframes.forEach(tf => {
            const trend = this.calcTrend(this.aggregate(data, tf));
            const cls = trend === 'UP' ? 'trend-up' : (trend === 'DOWN' ? 'trend-down' : '');
            trends += `<div class="tf-pill ${cls}">${tf}m</div>`;
        });

        card.innerHTML = `<div class="card-symbol">${symbol}</div><div class="card-price" style="color:${color}">${last.close.toFixed(5)}</div><div class="trend-labels">${trends}</div>`;
        card.onclick = () => {
            document.querySelector('.nav-item[data-page="charts"]')?.click();
            if (window.panes?.[0]) {
                window.panes[0].symbolSelect.value = symbol;
                window.panes[0].symbol = symbol;
                window.panes[0].fetchData();
            }
        };
    }

    aggregate(data, tf) {
        if (tf === 1) return data;
        const res = [], s = tf * 60; let cur = null;
        for (const i of data) {
            const bt = Math.floor(i.time / s) * s;
            if (!cur || cur.time !== bt) { if (cur) res.push(cur); cur = { ...i, time: bt }; }
            else { cur.high = Math.max(cur.high, i.high); cur.low = Math.min(cur.low, i.low); cur.close = i.close; }
        }
        if (cur) res.push(cur); return res;
    }

    calcTrend(data) {
        if (data.length < 10) return 'FLAT';
        const p = 5, zig = []; let lp = null;
        for (let i = p; i < data.length - p; i++) {
            const ch = data[i].high, cl = data[i].low; let isH = true, isL = true;
            for (let j = 1; j <= p; j++) { if (data[i - j].high >= ch || data[i + j].high > ch) isH = false; if (data[i - j].low <= cl || data[i + j].low < cl) isL = false; }
            if (isH && (!lp || lp.type === 'low' || ch > lp.value)) { if (lp?.type === 'high') zig.pop(); lp = { value: ch, type: 'high' }; zig.push(lp); }
            else if (isL && (!lp || lp.type === 'high' || cl < lp.value)) { if (lp?.type === 'low') zig.pop(); lp = { value: cl, type: 'low' }; zig.push(lp); }
        }
        if (zig.length < 4) return 'FLAT';
        const last = zig.slice(-4), h = last.filter(z => z.type === 'high'), l = last.filter(z => z.type === 'low');
        if (h.length >= 2 && l.length >= 2) {
            if (h[1].value > h[0].value && l[1].value > l[0].value) return 'UP';
            if (h[1].value < h[0].value && l[1].value < l[0].value) return 'DOWN';
        }
        return 'FLAT';
    }
}

// --- Chart Pane System ---
document.addEventListener('DOMContentLoaded', () => {
    const grid = document.getElementById('chart-container-grid');
    const template = document.getElementById('pane-template');
    const syncChannel = new BroadcastChannel('chart_sync');
    let isSyncing = false;
    let panes = []; window.panes = panes;

    if (!window.LightweightCharts) {
        showConnBanner('Chart library failed to load. Please refresh the page.');
        return;
    }

    const getPrefs = () => ({
        sma1: document.getElementById('check-sma20')?.checked || false,
        sma1_len: parseInt(document.getElementById('param-sma20')?.value) || 20,
        sma2: document.getElementById('check-sma50')?.checked || false,
        sma2_len: parseInt(document.getElementById('param-sma50')?.value) || 50,
        bb: document.getElementById('check-bb')?.checked || false,
        bb_period: parseInt(document.getElementById('param-bb-period')?.value) || 20,
        bb_dev: parseFloat(document.getElementById('param-bb-dev')?.value) || 2,
        rsi: document.getElementById('check-rsi')?.checked || false,
        rsi_len: parseInt(document.getElementById('param-rsi')?.value) || 14,
        macd: document.getElementById('check-macd')?.checked || false,
        macd_fast: parseInt(document.getElementById('param-macd-fast')?.value) || 12,
        macd_slow: parseInt(document.getElementById('param-macd-slow')?.value) || 26,
        macd_sig: parseInt(document.getElementById('param-macd-signal')?.value) || 9,
        macd2: document.getElementById('check-macd2')?.checked || false,
        macd2_fast: parseInt(document.getElementById('param-macd2-fast')?.value) || 19,
        macd2_slow: parseInt(document.getElementById('param-macd2-slow')?.value) || 39,
        macd2_sig: parseInt(document.getElementById('param-macd2-signal')?.value) || 9,
        dow: document.getElementById('check-dow')?.checked || false,
        dow_p: parseInt(document.getElementById('param-dow')?.value) || 5
    });

    class ChartPane {
        constructor(id, config = {}) {
            this.id = id;
            this.symbol = config.symbol || 'EURUSD';
            this.tf = config.tf || 5;
            this.isSync = config.isSync || false;
            this.data = [];
            this.charts = {};
            this.series = {};
            this.anchor = config.anchor ? new Date(config.anchor) : new Date('2025-04-01');
            this.isLoading = false;

            this.createUI();
            this.initCharts();
            this.bindEvents();
            this.fetchData();
        }

        createUI() {
            const clone = template.content.cloneNode(true);
            this.el = clone.querySelector('.pane');
            this.symbolSelect = this.el.querySelector('.symbol-select');
            this.tfSelect = this.el.querySelector('.timeframe-select');
            this.datePicker = this.el.querySelector('.date-picker');
            this.syncCheck = this.el.querySelector('.check-sync-scroll');
            this.loader = this.el.querySelector('.pane-loading-overlay');
            this.badge = this.el.querySelector('.trend-badge');

            this.symbolSelect.value = this.symbol;
            this.tfSelect.value = this.tf;
            this.datePicker.value = this.anchor.toISOString().split('T')[0];
            this.syncCheck.checked = this.isSync;

            grid.appendChild(this.el);
        }

        initCharts() {
            const opt = {
                layout: { background: { type: 'solid', color: '#131722' }, textColor: '#b2b5be' },
                grid: { vertLines: { color: '#1e222d' }, horzLines: { color: '#1e222d' } },
                timeScale: {
                    borderColor: '#2b2b43',
                    timeVisible: true,
                    tickMarkFormatter: (time) => {
                        const date = new Date(time * 1000);
                        const mm = String(date.getMonth() + 1).padStart(2, '0');
                        const dd = String(date.getDate()).padStart(2, '0');
                        const hh = String(date.getHours()).padStart(2, '0');
                        const min = String(date.getMinutes()).padStart(2, '0');
                        return `${mm}/${dd} ${hh}:${min}`;
                    }
                },
                rightPriceScale: { borderColor: '#2b2b43' }
            };

            this.charts.main = LightweightCharts.createChart(this.el.querySelector('.chart-container-el'), { ...opt, width: this.el.clientWidth, height: 400 });
            this.charts.rsi = LightweightCharts.createChart(this.el.querySelector('.rsi-chart-el'), { ...opt, width: this.el.clientWidth, height: 180 });
            this.charts.macd = LightweightCharts.createChart(this.el.querySelector('.macd-chart-el'), { ...opt, width: this.el.clientWidth, height: 180 });
            this.charts.macd2 = LightweightCharts.createChart(this.el.querySelector('.macd2-chart-el'), { ...opt, width: this.el.clientWidth, height: 180 });

            // Series with NO price lines
            this.series.candle = this.charts.main.addCandlestickSeries({
                upColor: '#10b981', downColor: '#ef4444',
                borderVisible: false,
                wickUpColor: '#10b981', wickDownColor: '#ef4444',
                priceLineVisible: false
            });
            this.series.sma1 = this.charts.main.addLineSeries({ color: '#3b82f6', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            this.series.sma2 = this.charts.main.addLineSeries({ color: '#f59e0b', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            this.series.bbUpper = this.charts.main.addLineSeries({ color: '#9333ea', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            this.series.bbMiddle = this.charts.main.addLineSeries({ color: '#9333ea', lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false });
            this.series.bbLower = this.charts.main.addLineSeries({ color: '#9333ea', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            this.series.dow = this.charts.main.addLineSeries({ color: '#8b5cf6', lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false });
            this.series.rsi = this.charts.rsi.addLineSeries({ color: '#f97316', lineWidth: 2, priceLineVisible: false });
            this.series.macd = this.charts.macd.addLineSeries({ color: '#3b82f6', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            this.series.macdSig = this.charts.macd.addLineSeries({ color: '#f59e0b', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            this.series.macdHist = this.charts.macd.addHistogramSeries({ color: '#10b981', priceLineVisible: false });

            this.series.macd2 = this.charts.macd2.addLineSeries({ color: '#8b5cf6', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            this.series.macd2Sig = this.charts.macd2.addLineSeries({ color: '#f59e0b', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
            this.series.macd2Hist = this.charts.macd2.addHistogramSeries({ color: '#10b981', priceLineVisible: false });

            // Crosshair sync across all charts
            Object.values(this.charts).forEach(c => {
                c.subscribeCrosshairMove(param => {
                    if (!param.time) {
                        Object.values(this.charts).forEach(oc => oc.clearCrosshairPosition());
                        return;
                    }
                    Object.values(this.charts).forEach(oc => {
                        if (oc !== c) oc.setCrosshairPosition(0, param.time, this.series.candle);
                    });
                });
            });

            // Sync all charts' time scales + infinite scroll
            Object.values(this.charts).forEach(c => {
                c.timeScale().subscribeVisibleLogicalRangeChange(range => {
                    if (isSyncing || !range) return;
                    isSyncing = true;
                    // Sync within this pane
                    Object.values(this.charts).forEach(oc => { if (oc !== c) oc.timeScale().setVisibleLogicalRange(range); });
                    // Sync across panes if enabled
                    if (this.isSync) {
                        const tr = c.timeScale().getVisibleRange();
                        if (tr) {
                            panes.forEach(p => { if (p !== this && p.isSync) p.syncToRange(tr); });
                            syncChannel.postMessage({ type: 'range', range: tr, pid: this.id });
                        }
                    }
                    // Infinite scroll: load more data when near edges
                    if (!this.isLoading && range.from < 50) this.loadMorePast();
                    if (!this.isLoading && this.data.length > 0) {
                        const tfData = this.aggregate(this.data, this.tf);
                        if (range.to > tfData.length - 50) this.loadMoreFuture();
                    }
                    isSyncing = false;
                });
            });
        }

        bindEvents() {
            this.symbolSelect.onchange = () => { this.symbol = this.symbolSelect.value; this.fetchData(); saveState(); };
            this.tfSelect.onchange = () => { this.tf = parseInt(this.tfSelect.value); this.refresh(); saveState(); };
            this.datePicker.onchange = () => { if (this.datePicker.value) { this.anchor = new Date(this.datePicker.value); this.fetchData(); saveState(); } };
            this.syncCheck.onchange = () => { this.isSync = this.syncCheck.checked; saveState(); };
            this.el.querySelector('.remove-pane-btn').onclick = () => this.destroy();
        }

        syncToRange(r) {
            isSyncing = true;
            Object.values(this.charts).forEach(c => { try { c.timeScale().setVisibleRange(r); } catch (e) { } });
            isSyncing = false;
        }

        async loadMorePast() {
            if (this.isLoading || !this.data.length) return;
            const oldestTime = this.data[0].time;
            const oldestDate = new Date(oldestTime * 1000);
            const startDate = new Date(oldestDate); startDate.setDate(startDate.getDate() - 3);
            const endDate = new Date(oldestDate); endDate.setDate(endDate.getDate() - 1);

            this.isLoading = true;
            try {
                const res = await fetch(`${API_BASE}/ohlc?symbol=${this.symbol}&start_date=${startDate.toISOString().split('T')[0]}&end_date=${endDate.toISOString().split('T')[0]}`);
                if (res.ok) {
                    const raw = await res.json();
                    if (raw?.length) {
                        const fmt = raw.map(i => ({ time: Math.floor(new Date(i.time).getTime() / 1000), open: i.open, high: i.high, low: i.low, close: i.close }))
                            .filter(d => !isNaN(d.time) && d.open && d.high && d.low && d.close);
                        this.data = [...fmt, ...this.data].sort((a, b) => a.time - b.time);
                        this.refresh();
                    }
                }
            } catch (e) { }
            finally { this.isLoading = false; }
        }

        async loadMoreFuture() {
            if (this.isLoading || !this.data.length) return;
            const newestTime = this.data[this.data.length - 1].time;
            const newestDate = new Date(newestTime * 1000);
            const startDate = new Date(newestDate); startDate.setDate(startDate.getDate() + 1);
            const endDate = new Date(newestDate); endDate.setDate(endDate.getDate() + 3);

            this.isLoading = true;
            try {
                const res = await fetch(`${API_BASE}/ohlc?symbol=${this.symbol}&start_date=${startDate.toISOString().split('T')[0]}&end_date=${endDate.toISOString().split('T')[0]}`);
                if (res.ok) {
                    const raw = await res.json();
                    if (raw?.length) {
                        const fmt = raw.map(i => ({ time: Math.floor(new Date(i.time).getTime() / 1000), open: i.open, high: i.high, low: i.low, close: i.close }))
                            .filter(d => !isNaN(d.time) && d.open && d.high && d.low && d.close);
                        this.data = [...this.data, ...fmt].sort((a, b) => a.time - b.time);
                        this.refresh();
                    }
                }
            } catch (e) { }
            finally { this.isLoading = false; }
        }

        async fetchData() {
            if (this.isLoading) return;
            this.isLoading = true;
            this.loader.style.display = 'flex';

            const s = new Date(this.anchor); s.setDate(s.getDate() - 7);
            const e = new Date(this.anchor); e.setDate(e.getDate() + 7);

            try {
                const res = await fetch(`${API_BASE}/ohlc?symbol=${this.symbol}&start_date=${s.toISOString().split('T')[0]}&end_date=${e.toISOString().split('T')[0]}`);
                if (!res.ok) throw new Error('Backend offline');
                const raw = await res.json();

                if (raw?.length) {
                    this.data = raw.map(i => ({
                        time: Math.floor(new Date(i.time).getTime() / 1000),
                        open: i.open, high: i.high, low: i.low, close: i.close
                    })).filter(d => !isNaN(d.time) && d.open && d.high && d.low && d.close)
                        .sort((a, b) => a.time - b.time);

                    this.refresh();
                    this.center();
                    hideConnBanner();
                }
            } catch (e) {
                showConnBanner('Failed to load data: ' + e.message);
            } finally {
                this.isLoading = false;
                this.loader.style.display = 'none';
            }
        }

        refresh() {
            if (!this.data.length) return;

            const tfData = this.aggregate(this.data, this.tf);
            const p = getPrefs();
            this.series.candle.setData(tfData);

            const closes = tfData.map(d => d.close);
            const times = tfData.map(d => d.time);

            // SMA
            const calcSMA = (len) => tfData.map((d, i) => {
                if (i < len - 1) return { time: d.time };
                let sum = 0;
                for (let j = 0; j < len; j++) sum += tfData[i - j].close;
                return { time: d.time, value: sum / len };
            });

            this.series.sma1.setData(p.sma1 ? calcSMA(p.sma1_len) : []);
            this.series.sma2.setData(p.sma2 ? calcSMA(p.sma2_len) : []);

            // Bollinger Bands
            if (p.bb) {
                const period = p.bb_period, dev = p.bb_dev;
                const bb = tfData.map((d, i) => {
                    if (i < period - 1) return { time: d.time };
                    const slice = tfData.slice(i - period + 1, i + 1);
                    const avg = slice.reduce((acc, x) => acc + x.close, 0) / period;
                    const variance = slice.reduce((acc, x) => acc + Math.pow(x.close - avg, 2), 0) / period;
                    const std = Math.sqrt(variance);
                    return { time: d.time, upper: avg + (dev * std), middle: avg, lower: avg - (dev * std) };
                });
                this.series.bbUpper.setData(bb.map(b => b.upper ? { time: b.time, value: b.upper } : { time: b.time }));
                this.series.bbMiddle.setData(bb.map(b => b.middle ? { time: b.time, value: b.middle } : { time: b.time }));
                this.series.bbLower.setData(bb.map(b => b.lower ? { time: b.time, value: b.lower } : { time: b.time }));
            } else {
                this.series.bbUpper.setData([]);
                this.series.bbMiddle.setData([]);
                this.series.bbLower.setData([]);
            }

            // RSI
            this.el.querySelector('.rsi-chart-wrapper').style.display = p.rsi ? 'block' : 'none';
            if (p.rsi) {
                const len = p.rsi_len;
                const rsi = tfData.map((d, i) => {
                    if (i < len) return { time: d.time };
                    let g = 0, l = 0;
                    for (let j = i - (len - 1); j <= i; j++) {
                        const df = tfData[j].close - tfData[j - 1].close;
                        if (df >= 0) g += df; else l -= df;
                    }
                    return { time: d.time, value: 100 - (100 / (1 + (g / (l || 0.001)))) };
                });
                this.series.rsi.setData(rsi);
            }

            // MACD 1
            this.el.querySelector('.macd-chart-wrapper').style.display = p.macd ? 'block' : 'none';
            if (p.macd) {
                const ema = (src, len) => {
                    const k = 2 / (len + 1);
                    let cur = src[0];
                    return src.map(v => { cur = (v * k) + (cur * (1 - k)); return cur; });
                };
                const f = ema(closes, p.macd_fast), s = ema(closes, p.macd_slow);
                const m = f.map((v, i) => v - s[i]);
                const sig = ema(m, p.macd_sig);
                this.series.macd.setData(m.map((v, i) => ({ time: times[i], value: v })));
                this.series.macdSig.setData(sig.map((v, i) => ({ time: times[i], value: v })));
                this.series.macdHist.setData(m.map((v, i) => ({
                    time: times[i],
                    value: v - sig[i],
                    color: v - sig[i] >= 0 ? '#10b981' : '#ef4444'
                })));
            }

            // MACD 2
            this.el.querySelector('.macd2-chart-wrapper').style.display = p.macd2 ? 'block' : 'none';
            if (p.macd2) {
                const ema = (src, len) => {
                    const k = 2 / (len + 1);
                    let cur = src[0];
                    return src.map(v => { cur = (v * k) + (cur * (1 - k)); return cur; });
                };
                const f = ema(closes, p.macd2_fast), s = ema(closes, p.macd2_slow);
                const m = f.map((v, i) => v - s[i]);
                const sig = ema(m, p.macd2_sig);
                this.series.macd2.setData(m.map((v, i) => ({ time: times[i], value: v })));
                this.series.macd2Sig.setData(sig.map((v, i) => ({ time: times[i], value: v })));
                this.series.macd2Hist.setData(m.map((v, i) => ({
                    time: times[i],
                    value: v - sig[i],
                    color: v - sig[i] >= 0 ? '#10b981' : '#ef4444'
                })));
            }

            // Dow with markers
            if (p.dow) {
                const dowPoints = this.calcDow(tfData, p.dow_p);
                this.series.dow.setData(dowPoints);

                // Add markers for HH/HL/LH/LL
                const markers = [];
                for (let i = 0; i < dowPoints.length; i++) {
                    const curr = dowPoints[i];
                    const prevSame = dowPoints.slice(0, i).reverse().find(z => z.type === curr.type);
                    let label = curr.type === 'high' ? 'H' : 'L';
                    if (prevSame) {
                        if (curr.type === 'high') label = curr.value > prevSame.value ? 'HH' : 'LH';
                        else label = curr.value < prevSame.value ? 'LL' : 'HL';
                    }
                    markers.push({
                        time: curr.time,
                        position: curr.type === 'high' ? 'aboveBar' : 'belowBar',
                        color: curr.type === 'high' ? '#8b5cf6' : '#06b6d4',
                        shape: curr.type === 'high' ? 'arrowDown' : 'arrowUp',
                        text: label,
                        size: 1
                    });
                }
                this.series.candle.setMarkers(markers);
            } else {
                this.series.dow.setData([]);
                this.series.candle.setMarkers([]);
            }

            this.badge.innerText = `${tfData.length} BARS LOADED`;
            this.resize();
        }

        calcDow(data, p) {
            const res = []; let lp = null;
            for (let i = p; i < data.length - p; i++) {
                const ch = data[i].high, cl = data[i].low;
                let isH = true, isL = true;
                for (let j = 1; j <= p; j++) {
                    if (data[i - j].high >= ch || data[i + j].high > ch) isH = false;
                    if (data[i - j].low <= cl || data[i + j].low < cl) isL = false;
                }
                if (isH && (!lp || lp.type === 'low' || ch > lp.value)) {
                    if (lp?.type === 'high') res.pop();
                    lp = { time: data[i].time, value: ch, type: 'high' };
                    res.push(lp);
                } else if (isL && (!lp || lp.type === 'high' || cl < lp.value)) {
                    if (lp?.type === 'low') res.pop();
                    lp = { time: data[i].time, value: cl, type: 'low' };
                    res.push(lp);
                }
            }
            return res;
        }

        aggregate(data, tf) {
            if (tf === 1) return data;
            const res = [], step = tf * 60; let cur = null;
            for (const i of data) {
                const bt = Math.floor(i.time / step) * step;
                if (!cur || cur.time !== bt) {
                    if (cur) res.push(cur);
                    cur = { ...i, time: bt };
                } else {
                    cur.high = Math.max(cur.high, i.high);
                    cur.low = Math.min(cur.low, i.low);
                    cur.close = i.close;
                }
            }
            if (cur) res.push(cur);
            return res;
        }

        resize() {
            const w = this.el.clientWidth;
            this.charts.main.resize(w, 400);
            this.charts.rsi.resize(w, 180);
            this.charts.macd.resize(w, 180);
            this.charts.macd2.resize(w, 180);
        }

        center() {
            const t = Math.floor(this.anchor.getTime() / 1000);
            const r = 100 * this.tf * 60;
            setTimeout(() => {
                try {
                    this.charts.main.timeScale().setVisibleRange({ from: t - r / 2, to: t + r / 2 });
                } catch (e) { }
            }, 100);
        }

        destroy() {
            Object.values(this.charts).forEach(c => c.remove());
            this.el.remove();
            const i = panes.indexOf(this);
            if (i > -1) panes.splice(i, 1);
            saveState();
        }
    }

    // Navigation
    let overview = null;
    document.querySelectorAll('.nav-item[data-page]').forEach(n => n.onclick = () => {
        const p = n.dataset.page;
        document.querySelectorAll('.nav-item').forEach(x => x.classList.toggle('active', x === n));
        document.querySelectorAll('.page-container').forEach(x => x.classList.toggle('active', x.id === `page-${p}`));
        if (p === 'dashboard' && !overview) overview = new MarketOverview();
        if (p === 'charts') setTimeout(() => panes.forEach(p => p.resize()), 150);
    });

    document.getElementById('add-pane-btn').onclick = () => { panes.push(new ChartPane(Date.now())); saveState(); };
    document.querySelectorAll('.toolbar input').forEach(el => el.onchange = () => { panes.forEach(p => p.refresh()); saveState(); });

    // Cross-tab sync
    syncChannel.onmessage = e => {
        if (isSyncing) return;
        const { type, range, pid } = e.data;
        if (type === 'range' && range) panes.forEach(p => { if (p.id !== pid && p.isSync) p.syncToRange(range); });
    };

    // Initialize
    overview = new MarketOverview();

    // Restore state or create default
    const saved = loadState();
    if (saved?.panes?.length) {
        saved.panes.forEach(cfg => panes.push(new ChartPane(Date.now() + Math.random(), cfg)));
        // Restore prefs
        if (saved.prefs) {
            // Restore checkboxes
            const checks = ['sma1', 'sma2', 'bb', 'rsi', 'macd', 'macd2', 'dow'];
            const map = {
                sma1: 'check-sma20', sma2: 'check-sma50', bb: 'check-bb',
                rsi: 'check-rsi', macd: 'check-macd', macd2: 'check-macd2', dow: 'check-dow'
            };
            checks.forEach(k => {
                const el = document.getElementById(map[k]);
                if (el && saved.prefs[k] !== undefined) el.checked = saved.prefs[k];
            });

            // Restore params
            const params = {
                sma1_val: 'param-sma20', sma2_val: 'param-sma50',
                bb_p: 'param-bb-period', bb_d: 'param-bb-dev',
                rsi_val: 'param-rsi',
                macd_f: 'param-macd-fast', macd_s: 'param-macd-slow', macd_sig: 'param-macd-signal',
                macd2_f: 'param-macd2-fast', macd2_s: 'param-macd2-slow', macd2_sig: 'param-macd2-signal',
                dow_p: 'param-dow'
            };
            Object.entries(params).forEach(([k, id]) => {
                const el = document.getElementById(id);
                if (el && saved.prefs[k]) el.value = saved.prefs[k];
            });
        }
    } else {
        panes.push(new ChartPane(Date.now()));
    }
});
