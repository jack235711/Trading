document.addEventListener('DOMContentLoaded', () => {
    const mainWrapper = document.getElementById('main-area');
    const rsiWrapper = document.getElementById('rsi-area');
    const macdWrapper = document.getElementById('macd-area');
    const statusMsg = document.getElementById('status-msg');

    let allData = [];
    let earliestDate = new Date('2025-01-29');
    let isLoading = false;

    // シリーズ保持
    let mainChart, rsiChart, macdChart;
    let candlestickSeries;
    let sma20Series, sma50Series, bbUpperSeries, bbLowerSeries;
    let rsiSeries, rsi70Series, rsi30Series;
    let macdSeries, macdSignalSeries, macdHistogramSeries;

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
            priceFormat: priceFormat5
        });
        sma20Series = mainChart.addLineSeries({ color: '#2962FF', lineWidth: 1, title: 'SMA 1', priceFormat: priceFormat5 });
        sma50Series = mainChart.addLineSeries({ color: '#FF6D00', lineWidth: 1, title: 'SMA 2', priceFormat: priceFormat5 });
        bbUpperSeries = mainChart.addLineSeries({ color: 'rgba(38, 166, 154, 0.4)', lineWidth: 1, lineStyle: 2, priceFormat: priceFormat5 });
        bbLowerSeries = mainChart.addLineSeries({ color: 'rgba(38, 166, 154, 0.4)', lineWidth: 1, lineStyle: 2, priceFormat: priceFormat5 });

        // RSIチャート
        rsiChart = LightweightCharts.createChart(document.getElementById('rsi-chart'), { ...options, height: 150 });
        rsiSeries = rsiChart.addLineSeries({ color: '#f7702d', lineWidth: 2, title: 'RSI' });
        rsi70Series = rsiChart.addLineSeries({ color: 'rgba(239, 83, 80, 0.3)', lineWidth: 1, lineStyle: 2 });
        rsi30Series = rsiChart.addLineSeries({ color: 'rgba(38, 166, 154, 0.3)', lineWidth: 1, lineStyle: 2 });

        // MACDチャート
        macdChart = LightweightCharts.createChart(document.getElementById('macd-chart'), { ...options, height: 150 });
        const macdFormat = { type: 'price', precision: 6, minMove: 0.000001 };

        macdSeries = macdChart.addLineSeries({ color: '#2962ff', lineWidth: 1, title: 'MACD', priceFormat: macdFormat });
        macdSignalSeries = macdChart.addLineSeries({ color: '#ff6d00', lineWidth: 1, title: 'Signal', priceFormat: macdFormat });
        macdHistogramSeries = macdChart.addHistogramSeries({ color: '#26a69a', base: 0, priceFormat: macdFormat });

        // 同期ロジック
        [mainChart, rsiChart, macdChart].forEach(c => {
            c.timeScale().subscribeVisibleLogicalRangeChange(range => {
                [mainChart, rsiChart, macdChart].forEach(target => {
                    if (target !== c) target.timeScale().setVisibleLogicalRange(range);
                });
            });
        });

        mainChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
            if (range && range.from < 50 && !isLoading) fetchMoreData();
        });
    }
    let aggregatedCache = {}; // 時間足ごとのキャッシュ { timeframe: data }

    // データを任意の時間足に集計 (キャッシュ対応)
    function aggregateData(data, timeframeMinutes) {
        if (timeframeMinutes === 1) return data;

        // キャッシュに存在し、かつデータ長が同じ場合は再利用
        const cacheKey = timeframeMinutes;
        if (aggregatedCache[cacheKey] && aggregatedCache[cacheKey].sourceLength === data.length) {
            return aggregatedCache[cacheKey].data;
        }

        const result = [];
        const interval = timeframeMinutes * 60;

        let currentBar = null;
        for (let i = 0; i < data.length; i++) {
            const item = data[i];
            const barTime = Math.floor(item.time / interval) * interval;
            if (!currentBar || currentBar.time !== barTime) {
                if (currentBar) result.push(currentBar);
                currentBar = {
                    time: barTime,
                    open: item.open,
                    high: item.high,
                    low: item.low,
                    close: item.close
                };
            } else {
                currentBar.high = Math.max(currentBar.high, item.high);
                currentBar.low = Math.min(currentBar.low, item.low);
                currentBar.close = item.close;
            }
        }
        if (currentBar) result.push(currentBar);

        // キャッシュに保存
        aggregatedCache[cacheKey] = {
            data: result,
            sourceLength: data.length
        };

        return result;
    }

    let calculationTimeout = null;
    function calculateIndicators(data) {
        if (!data || data.length === 0) return;

        // 連続呼び出しを抑制 (Debounce)
        if (calculationTimeout) clearTimeout(calculationTimeout);
        calculationTimeout = setTimeout(() => {
            // パネル表示制御とリサイズ
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
            const closes = new Float64Array(n);
            const times = new Int32Array(n);
            for (let i = 0; i < n; i++) {
                closes[i] = aggrData[i].close;
                times[i] = aggrData[i].time;
            }

            // DOMアクセスを一度にまとめる
            const checks = {
                sma1: document.getElementById('check-sma20').checked,
                sma2: document.getElementById('check-sma50').checked,
                bb: document.getElementById('check-bb').checked,
                rsi: document.getElementById('check-rsi').checked,
                macd: document.getElementById('check-macd').checked
            };
            const params = {
                sma1: parseInt(document.getElementById('param-sma20').value) || 20,
                sma2: parseInt(document.getElementById('param-sma50').value) || 50,
                bbP: parseInt(document.getElementById('param-bb-period').value) || 20,
                bbD: parseFloat(document.getElementById('param-bb-dev').value) || 2,
                rsiP: parseInt(document.getElementById('param-rsi').value) || 14,
                mF: parseInt(document.getElementById('param-macd-fast').value) || 12,
                mS: parseInt(document.getElementById('param-macd-slow').value) || 26,
                mSig: parseInt(document.getElementById('param-macd-signal').value) || 9
            };

            const calcSMA = p => {
                const results = [];
                for (let i = p - 1; i < n; i++) {
                    let sum = 0;
                    for (let j = 0; j < p; j++) sum += closes[i - j];
                    results.push({ time: times[i], value: sum / p });
                }
                return results;
            };

            const calcEMA = (p, sourceData = closes) => {
                const results = [];
                const k = 2 / (p + 1);
                let ema = sourceData[0]; // Initialize with the first value
                for (let i = 0; i < n; i++) {
                    // Ensure sourceData[i] exists, otherwise use previous ema
                    const currentVal = sourceData[i] !== undefined ? sourceData[i] : ema;
                    ema = (currentVal * k) + (ema * (1 - k));
                    results.push({ time: times[i], value: ema });
                }
                return results;
            };

            sma20Series.setData(checks.sma1 ? calcSMA(params.sma1) : []);
            sma50Series.setData(checks.sma2 ? calcSMA(params.sma2) : []);

            if (checks.bb) {
                const p = params.bbP, d = params.bbD;
                const up = [], lo = [];
                for (let i = p - 1; i < n; i++) {
                    let sum = 0, sqSum = 0;
                    for (let j = 0; j < p; j++) {
                        const val = closes[i - j];
                        sum += val;
                        sqSum += val * val;
                    }
                    const avg = sum / p;
                    const std = Math.sqrt(Math.max(0, (sqSum / p) - (avg * avg)));
                    up.push({ time: times[i], value: avg + d * std });
                    lo.push({ time: times[i], value: avg - d * std });
                }
                bbUpperSeries.setData(up); bbLowerSeries.setData(lo);
            } else { bbUpperSeries.setData([]); bbLowerSeries.setData([]); }

            if (updatePanel('check-rsi', rsiWrapper, rsiChart)) {
                const p = params.rsiP;
                const rsi = [];
                for (let i = p; i < n; i++) {
                    let g = 0, l = 0;
                    for (let j = i - p + 1; j <= i; j++) {
                        const diff = closes[j] - closes[j - 1];
                        if (diff >= 0) g += diff; else l -= diff;
                    }
                    rsi.push({ time: times[i], value: 100 - (100 / (1 + (g / (l || 0.0001)))) });
                }
                rsiSeries.setData(rsi);
                const bg = rsi.map(d => ({ time: d.time, value: 70 }));
                rsi70Series.setData(bg);
                rsi30Series.setData(bg.map(d => ({ ...d, value: 30 })));
            }

            if (checks.macd && updatePanel('check-macd', macdWrapper, macdChart)) {
                const fEma = calcEMA(params.mF), sEma = calcEMA(params.mS);
                const macdLine = fEma.map((f, i) => ({ time: f.time, value: f.value - sEma[i].value }));
                const sigLine = calcEMA(params.mSig, macdLine.map(m => m.value));
                const hist = macdLine.map((m, i) => {
                    const val = m.value - sigLine[i].value;
                    return { time: times[i], value: val, color: val >= 0 ? '#26a69a' : '#ef5350' };
                });
                macdSeries.setData(macdLine); macdSignalSeries.setData(sigLine); macdHistogramSeries.setData(hist);
            }
        }, 50); // 50msのバッファ
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
                aggregatedCache = {}; // データが更新されたのでキャッシュをクリア
                calculateIndicators(allData);
            }
            earliestDate = targetDate; isLoading = false; hideStatus();
            if (newData.length === 0 && earliestDate > new Date('2025-01-01')) {
                // データがない場合は少し待ってから次を試行（サーバー負荷とブラウザのハング防止）
                setTimeout(() => fetchMoreData(), 100);
            }
        } catch (e) {
            showStatus(`サーバー接続エラー: ${e.message}`, true);
            isLoading = false;
            // エラー時は自動再開せず、ユーザーの操作を待つ
        }
    }

    async function initialLoad() {
        initCharts();
        // 現在ダウンロード済みの最新データ（2月〜3月等）も表示できるよう、将来の日付から遡って検索を開始
        earliestDate = new Date('2025-04-01');
        for (let i = 0; i < 5; i++) await fetchMoreData();
        mainChart.timeScale().fitContent();
    }

    window.addEventListener('resize', () => {
        const w = mainWrapper.clientWidth;
        [mainChart, rsiChart, macdChart].forEach(c => c.resize(w, c.options ? c.options().height : undefined));
    });

    // スケール・表示設定の更新
    function updateChartSettings() {
        const scaleType = document.getElementById('scale-type').value;
        const priceMode = document.getElementById('price-mode').value;

        const isAuto = scaleType === 'auto';
        const mode = priceMode === 'log' ? LightweightCharts.PriceScaleMode.Logarithmic : LightweightCharts.PriceScaleMode.Normal;

        mainChart.priceScale('right').applyOptions({
            autoScale: isAuto,
            mode: mode
        });
    }

    const inputs = ['check-sma20', 'check-sma50', 'check-bb', 'check-rsi', 'check-macd', 'param-sma20', 'param-sma50', 'param-bb-period', 'param-bb-dev', 'param-rsi', 'param-macd-fast', 'param-macd-slow', 'param-macd-signal'];
    inputs.forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener(el.type === 'checkbox' ? 'change' : 'input', () => calculateIndicators(allData));
    });

    // 時間足・スケール設定のイベントリスナー
    ['timeframe', 'scale-type', 'price-mode'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('change', () => {
            if (id === 'timeframe') calculateIndicators(allData);
            else updateChartSettings();
        });
    });

    initialLoad();
});
