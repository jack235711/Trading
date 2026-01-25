document.addEventListener('DOMContentLoaded', () => {
    const mainWrapper = document.getElementById('main-area');
    const rsiWrapper = document.getElementById('rsi-area');
    const macdWrapper = document.getElementById('macd-area');
    const statusMsg = document.getElementById('status-msg');

    let allData = [];
    let earliestDate = new Date('2025-01-29');
    let isLoading = false;
    let isSyncing = false;
    const syncChannel = new BroadcastChannel('chart_sync');

    // シリーズ保持
    let mainChart, rsiChart, macdChart;
    let candlestickSeries;
    let sma20Series, sma50Series, bbUpperSeries, bbLowerSeries;
    let rsiSeries, rsi70Series, rsi30Series;
    let macdSeries, macdSignalSeries, macdHistogramSeries;
    let dowSeries;

    function showStatus(msg, isError = false) {
        statusMsg.innerText = msg;
        statusMsg.style.display = 'block';
        statusMsg.style.color = isError ? '#ef5350' : '#787b86';
        if (isError) console.error(msg);
    }

    function hideStatus() {
        statusMsg.style.display = 'none';
    }

    function initCharts() {
        const options = {
            width: mainWrapper.clientWidth,
            layout: { background: { color: '#1e222d' }, textColor: '#d1d4dc' },
            grid: { vertLines: { color: '#2b2b43' }, horzLines: { color: '#2b2b43' } },
            timeScale: { borderColor: '#2b2b43', timeVisible: true }
        };

        // メインチャート
        mainChart = LightweightCharts.createChart(document.getElementById('chart'), { ...options, height: 450 });
        const priceFormat5 = { type: 'price', precision: 5, minMove: 0.00001 };

        candlestickSeries = mainChart.addCandlestickSeries({
            upColor: '#26a69a', downColor: '#ef5350', borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350',
            priceFormat: priceFormat5,
            priceLineVisible: false
        });
        sma20Series = mainChart.addLineSeries({ color: '#2962FF', lineWidth: 1, title: 'SMA 1', priceLineVisible: false, priceFormat: priceFormat5 });
        sma50Series = mainChart.addLineSeries({ color: '#FF6D00', lineWidth: 1, title: 'SMA 2', priceLineVisible: false, priceFormat: priceFormat5 });
        bbUpperSeries = mainChart.addLineSeries({ color: 'rgba(38, 166, 154, 0.4)', lineWidth: 1, priceLineVisible: false, priceFormat: priceFormat5 });
        bbLowerSeries = mainChart.addLineSeries({ color: 'rgba(38, 166, 154, 0.4)', lineWidth: 1, priceLineVisible: false, priceFormat: priceFormat5 });
        dowSeries = mainChart.addLineSeries({ color: '#facc15', lineWidth: 1, lineStyle: 2, title: 'Dow', priceLineVisible: false, priceFormat: priceFormat5 });

        // RSIチャート
        rsiChart = LightweightCharts.createChart(document.getElementById('rsi-chart'), { ...options, height: 150 });
        rsiSeries = rsiChart.addLineSeries({ color: '#f7702d', lineWidth: 2, title: 'RSI', priceLineVisible: false });
        rsi70Series = rsiChart.addLineSeries({ color: 'rgba(239, 83, 80, 0.3)', lineWidth: 1, priceLineVisible: false });
        rsi30Series = rsiChart.addLineSeries({ color: 'rgba(38, 166, 154, 0.3)', lineWidth: 1, priceLineVisible: false });

        // MACDチャート
        macdChart = LightweightCharts.createChart(document.getElementById('macd-chart'), { ...options, height: 150 });
        const macdFormat = { type: 'price', precision: 6, minMove: 0.000001 };
        macdSeries = macdChart.addLineSeries({ color: '#2962ff', lineWidth: 1, title: 'MACD', priceLineVisible: false, priceFormat: macdFormat });
        macdSignalSeries = macdChart.addLineSeries({ color: '#ff6d00', lineWidth: 1, title: 'Signal', priceLineVisible: false, priceFormat: macdFormat });
        macdHistogramSeries = macdChart.addHistogramSeries({ color: '#26a69a', base: 0, priceLineVisible: false, priceFormat: macdFormat });

        // レイアウト同期: 価格目盛りの幅を 100px に固定
        [mainChart, rsiChart, macdChart].forEach(c => {
            c.priceScale('right').applyOptions({ minimumWidth: 100, maximumWidth: 100 });
        });

        // 同期ロジック
        const charts = [mainChart, rsiChart, macdChart];
        const seriesList = [candlestickSeries, rsiSeries, macdSeries];
        const targetPrices = [null, 50, 0];

        charts.forEach((c, index) => {
            // 時間軸の同期 (LogicalRangeを使用することで、未ロード領域でも安定して同期)
            c.timeScale().subscribeVisibleLogicalRangeChange(range => {
                if (isSyncing || !range) return;
                isSyncing = true;

                charts.forEach(target => {
                    if (target !== c) {
                        target.timeScale().setVisibleLogicalRange(range);
                    }
                });

                // 他のタブを同期するためのデータをブロードキャスト (これはTimeRangeで行うのが一般的)
                const timeRange = c.timeScale().getVisibleRange();
                if (timeRange) {
                    syncChannel.postMessage({
                        type: 'range',
                        range: timeRange
                    });
                }

                isSyncing = false;
            });

            // 十字カーソルの同期
            c.subscribeCrosshairMove(param => {
                if (isSyncing) return;
                isSyncing = true;

                const time = param.time;
                charts.forEach((target, tIdx) => {
                    if (target === c) return;

                    if (!time) {
                        target.setCrosshairPosition(0, 0, seriesList[tIdx]);
                    } else {
                        // 価格位置。メインチャート以外は固定位置(中央など)に表示
                        const price = targetPrices[tIdx] === null ? (param.point ? param.point.y : 0) : targetPrices[tIdx];
                        target.setCrosshairPosition(price, time, seriesList[tIdx]);
                    }
                });

                // 他のタブを同期
                syncChannel.postMessage({
                    type: 'crosshair',
                    time: time || null
                });

                isSyncing = false;
            });
        });

        // データの追加読み込み (メインチャートのスクロールを検知)
        mainChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
            if (range && range.from < 50 && !isLoading) fetchMoreData();
        });

        // 他のタブからの同期メッセージをリッスン
        syncChannel.onmessage = (event) => {
            if (isSyncing) return;
            isSyncing = true;

            const { type, range, time } = event.data;

            if (type === 'range' && range) {
                charts.forEach(c => c.timeScale().setVisibleRange(range));
            } else if (type === 'crosshair') {
                charts.forEach((target, tIdx) => {
                    if (!time) {
                        target.setCrosshairPosition(0, 0, seriesList[tIdx]);
                    } else {
                        const price = targetPrices[tIdx] === null ? 0 : targetPrices[tIdx];
                        target.setCrosshairPosition(price, time, seriesList[tIdx]);
                    }
                });
            }

            isSyncing = false;
        };
    }

    let aggregatedCache = {};
    function aggregateData(data, timeframeMinutes) {
        if (timeframeMinutes === 1) return data;
        const cacheKey = timeframeMinutes;
        if (aggregatedCache[cacheKey] && aggregatedCache[cacheKey].sourceLength === data.length) return aggregatedCache[cacheKey].data;
        const result = [];
        const interval = timeframeMinutes * 60;
        let currentBar = null;
        for (let i = 0; i < data.length; i++) {
            const item = data[i];
            const barTime = Math.floor(item.time / interval) * interval;
            if (!currentBar || currentBar.time !== barTime) {
                if (currentBar) result.push(currentBar);
                currentBar = { time: barTime, open: item.open, high: item.high, low: item.low, close: item.close };
            } else {
                currentBar.high = Math.max(currentBar.high, item.high);
                currentBar.low = Math.min(currentBar.low, item.low);
                currentBar.close = item.close;
            }
        }
        if (currentBar) result.push(currentBar);
        aggregatedCache[cacheKey] = { data: result, sourceLength: data.length };
        return result;
    }

    let calculationTimeout = null;
    function calculateIndicators(data) {
        if (!data || data.length === 0) return;
        if (calculationTimeout) clearTimeout(calculationTimeout);
        calculationTimeout = setTimeout(() => {
            const updatePanel = (checkId, areaEl, chartObj) => {
                const show = document.getElementById(checkId).checked;
                const currentDisplay = areaEl.style.display;
                areaEl.style.display = show ? 'block' : 'none';
                if (show && currentDisplay === 'none') {
                    setTimeout(() => chartObj.resize(areaEl.clientWidth, areaEl.offsetHeight || 150), 10);
                }
                return show;
            };

            const timeframe = parseInt(document.getElementById('timeframe').value) || 1;
            const aggrData = aggregateData(data, timeframe);
            candlestickSeries.setData(aggrData);

            const n = aggrData.length;
            const highs = aggrData.map(d => d.high);
            const lows = aggrData.map(d => d.low);
            const closes = aggrData.map(d => d.close);
            const times = aggrData.map(d => d.time);

            const checks = {
                sma1: document.getElementById('check-sma20').checked,
                sma2: document.getElementById('check-sma50').checked,
                bb: document.getElementById('check-bb').checked,
                rsi: document.getElementById('check-rsi').checked,
                macd: document.getElementById('check-macd').checked,
                dow: document.getElementById('check-dow').checked
            };
            const params = {
                sma1: parseInt(document.getElementById('param-sma20').value) || 20,
                sma2: parseInt(document.getElementById('param-sma50').value) || 50,
                bbP: parseInt(document.getElementById('param-bb-period').value) || 20,
                bbD: parseFloat(document.getElementById('param-bb-dev').value) || 2,
                rsiP: parseInt(document.getElementById('param-rsi').value) || 14,
                mF: parseInt(document.getElementById('param-macd-fast').value) || 12,
                mS: parseInt(document.getElementById('param-macd-slow').value) || 26,
                mSig: parseInt(document.getElementById('param-macd-signal').value) || 9,
                dowP: parseInt(document.getElementById('param-dow').value) || 5
            };

            const calcSMA = p => {
                const results = [];
                for (let i = 0; i < n; i++) {
                    if (i < p - 1) results.push({ time: times[i] });
                    else {
                        let sum = 0;
                        for (let j = 0; j < p; j++) sum += closes[i - j];
                        results.push({ time: times[i], value: sum / p });
                    }
                }
                return results;
            };

            const calcEMA = (p, sourceData = closes) => {
                const results = [];
                const k = 2 / (p + 1);
                let ema = null;
                for (let i = 0; i < n; i++) {
                    const val = typeof sourceData[i] === 'object' ? sourceData[i].value : sourceData[i];
                    if (val === undefined || val === null) { results.push({ time: times[i] }); continue; }
                    if (ema === null) ema = val;
                    else ema = (val * k) + (ema * (1 - k));
                    results.push({ time: times[i], value: ema });
                }
                return results;
            };

            sma20Series.setData(checks.sma1 ? calcSMA(params.sma1) : []);
            sma50Series.setData(checks.sma2 ? calcSMA(params.sma2) : []);

            if (checks.bb) {
                const p = params.bbP, d = params.bbD;
                const up = [], lo = [];
                for (let i = 0; i < n; i++) {
                    if (i < p - 1) { up.push({ time: times[i] }); lo.push({ time: times[i] }); }
                    else {
                        let sum = 0, sqSum = 0;
                        for (let j = 0; j < p; j++) { const val = closes[i - j]; sum += val; sqSum += val * val; }
                        const avg = sum / p;
                        const std = Math.sqrt(Math.max(0, (sqSum / p) - (avg * avg)));
                        up.push({ time: times[i], value: avg + d * std }); lo.push({ time: times[i], value: avg - d * std });
                    }
                }
                bbUpperSeries.setData(up); bbLowerSeries.setData(lo);
            } else { bbUpperSeries.setData([]); bbLowerSeries.setData([]); }

            if (updatePanel('check-rsi', rsiWrapper, rsiChart)) {
                const p = params.rsiP, rsi = [];
                for (let i = 0; i < n; i++) {
                    if (i < p) rsi.push({ time: times[i] });
                    else {
                        let g = 0, l = 0;
                        for (let j = i - p + 1; j <= i; j++) { const diff = closes[j] - closes[j - 1]; if (diff >= 0) g += diff; else l -= diff; }
                        rsi.push({ time: times[i], value: 100 - (100 / (1 + (g / (l || 0.0001)))) });
                    }
                }
                rsiSeries.setData(rsi);
                const bg = times.map(t => ({ time: t, value: 70 }));
                rsi70Series.setData(bg); rsi30Series.setData(bg.map(d => ({ ...d, value: 30 })));
            }
            if (checks.macd && updatePanel('check-macd', macdWrapper, macdChart)) {
                const fEma = calcEMA(params.mF), sEma = calcEMA(params.mS);
                const macdLine = fEma.map((f, i) => ({ time: f.time, value: (f.value !== undefined && sEma[i].value !== undefined) ? f.value - sEma[i].value : undefined }));
                const sigLine = calcEMA(params.mSig, macdLine);
                const hist = macdLine.map((m, i) => {
                    const val = (m.value !== undefined && sigLine[i].value !== undefined) ? m.value - sigLine[i].value : undefined;
                    return { time: times[i], value: val, color: val >= 0 ? '#26a69a' : '#ef5350' };
                });
                macdSeries.setData(macdLine); macdSignalSeries.setData(sigLine); macdHistogramSeries.setData(hist);
            }
            if (checks.dow) {
                const p = params.dowP, zigzags = [], markers = [];
                let lastPoint = null;
                for (let i = p; i < n - p; i++) {
                    const ch = highs[i], cl = lows[i];
                    let isH = true, isL = true;
                    for (let j = 1; j <= p; j++) { if (highs[i - j] >= ch || highs[i + j] > ch) isH = false; if (lows[i - j] <= cl || lows[i + j] < cl) isL = false; }
                    if (isH && (!lastPoint || lastPoint.type === 'low' || ch > lastPoint.value)) {
                        const pt = { time: times[i], value: ch, type: 'high' }; if (lastPoint && lastPoint.type === 'high') zigzags.pop();
                        zigzags.push(pt); markers.push({ time: times[i], position: 'aboveBar', color: '#eab308', shape: 'arrowDown', text: 'H' }); lastPoint = pt;
                    } else if (isL && (!lastPoint || lastPoint.type === 'high' || cl < lastPoint.value)) {
                        const pt = { time: times[i], value: cl, type: 'low' }; if (lastPoint && lastPoint.type === 'low') zigzags.pop();
                        zigzags.push(pt); markers.push({ time: times[i], position: 'belowBar', color: '#6366f1', shape: 'arrowUp', text: 'L' }); lastPoint = pt;
                    }
                }
                dowSeries.setData(zigzags); candlestickSeries.setMarkers(markers);
            } else { dowSeries.setData([]); candlestickSeries.setMarkers([]); }
        }, 50);
    }

    async function fetchMoreData() {
        if (isLoading) return;
        isLoading = true;
        const targetDate = new Date(earliestDate); targetDate.setDate(targetDate.getDate() - 1);
        const dateStr = targetDate.toISOString().split('T')[0];
        showStatus(`過去データを読み込み中: ${dateStr}...`);
        try {
            const response = await fetch(`http://localhost:8000/ohlc?start_date=${dateStr}`);
            const newData = await response.json();
            if (newData && newData.length > 0) {
                const formatted = newData.map(i => ({ time: Math.floor(new Date(i.time).getTime() / 1000), open: i.open, high: i.high, low: i.low, close: i.close }));
                allData = [...formatted, ...allData].sort((a, b) => a.time - b.time);
                const unique = []; let last = null; for (const d of allData) { if (d.time !== last) { unique.push(d); last = d.time; } }
                allData = unique;
                aggregatedCache = {};
                calculateIndicators(allData);
            }
            earliestDate = targetDate; isLoading = false; hideStatus();
            if (newData.length === 0 && earliestDate > new Date('2025-01-01')) setTimeout(() => fetchMoreData(), 100);
        } catch (e) { showStatus(`サーバー接続エラー: ${e.message}`, true); isLoading = false; }
    }

    async function initialLoad() {
        initCharts();
        earliestDate = new Date('2025-04-01');
        // 初期データを数日分読み込む
        for (let i = 0; i < 3; i++) await fetchMoreData();

        // すべてのチャートの表示範囲を揃える
        const mainRange = mainChart.timeScale().getVisibleRange();
        if (mainRange) {
            [rsiChart, macdChart].forEach(c => c.timeScale().setVisibleRange(mainRange));
        }
    }

    window.addEventListener('resize', () => {
        const w = mainWrapper.clientWidth;
        [mainChart, rsiChart, macdChart].forEach(c => c.resize(w, c.options ? c.options().height : undefined));
    });

    function updateChartSettings() {
        const scaleType = document.getElementById('scale-type').value;
        const priceMode = document.getElementById('price-mode').value;
        const isAuto = scaleType === 'auto';
        const mode = priceMode === 'log' ? LightweightCharts.PriceScaleMode.Logarithmic : LightweightCharts.PriceScaleMode.Normal;
        mainChart.priceScale('right').applyOptions({ autoScale: isAuto, mode: mode });
    }

    const inputIds = ['check-sma20', 'check-sma50', 'check-bb', 'check-rsi', 'check-macd', 'check-dow', 'param-sma20', 'param-sma50', 'param-bb-period', 'param-bb-dev', 'param-rsi', 'param-macd-fast', 'param-macd-slow', 'param-macd-signal', 'param-dow'];
    inputIds.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener(el.type === 'checkbox' ? 'change' : 'input', () => calculateIndicators(allData));
    });

    ['timeframe', 'scale-type', 'price-mode'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('change', () => { if (id === 'timeframe') calculateIndicators(allData); else updateChartSettings(); });
    });

    initialLoad();
});
