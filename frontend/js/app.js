const API = window.location.origin;
let ws = null;
let currentCall = null;
let transcripts = {};

function toast(msg, type) {
  const el = document.createElement('div');
  el.className = 'toast toast-' + (type||'success');
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 3000);
}

let wsReconnectTimer = null;

function connectWS() {
  if (ws) { ws.close(); ws = null; }
  if (wsReconnectTimer) { clearTimeout(wsReconnectTimer); wsReconnectTimer = null; }
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = proto + '//' + location.host + '/ws/realtime';
  try {
    ws = new WebSocket(url);
    ws.onopen = () => {
      document.getElementById('ws-status').textContent = '已连接';
      document.getElementById('ws-status').className = 'status status-on';
    };
    ws.onclose = () => {
      document.getElementById('ws-status').textContent = '离线';
      document.getElementById('ws-status').className = 'status status-off';
      wsReconnectTimer = setTimeout(() => connectWS(), 3000);
    };
    ws.onerror = () => {
      ws.close();
    };
    ws.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data);
        if (data.type === 'call_start') addCallStartItem(data);
        if (data.type === 'voice_analysis') addFeedItem(data);
        if (data.type === 'simulation_end') {
          var liveInd = document.getElementById('live-indicator');
          var liveIdle = document.getElementById('live-idle');
          if (liveInd) liveInd.style.display = 'none';
          if (liveIdle) liveIdle.style.display = '';
          if (liveIdle) liveIdle.textContent = '等待中';
          if (!_simStopped) {
            toast(data.message || '模拟结束，数据已写入存储');
          }
          _simStopped = false;
          loadOverview();
        }
      } catch(ex) {}
    };
  } catch(ex) {
    document.getElementById('ws-status').textContent = '离线';
    document.getElementById('ws-status').className = 'status status-off';
    wsReconnectTimer = setTimeout(() => connectWS(), 3000);
  }
}

function addCallStartItem(data) {
  var liveInd = document.getElementById('live-indicator');
  var liveIdle = document.getElementById('live-idle');
  if (liveInd) liveInd.style.display = '';
  if (liveIdle) liveIdle.style.display = 'none';
  const feed = document.getElementById('realtime-feed');
  const div = document.createElement('div');
  div.id = 'callstart-' + data.call_id;
  div.className = 'feed-item';
  div.style.opacity = '0.6';
  div.innerHTML = `
    <div class="feed-header">
      <strong>📞 ${data.customer || data.call_id}</strong>
      <span style="color:var(--accent)">正在分析 (${data.index}/${data.total})</span>
    </div>
    <div style="font-size:12px;color:var(--muted);margin-top:4px">通话接入中，LLM 分析进行中...</div>
  `;
  feed.insertBefore(div, feed.firstChild);
}

function addFeedItem(data) {
  var liveInd = document.getElementById('live-indicator');
  var liveIdle = document.getElementById('live-idle');
  if (liveInd) liveInd.style.display = '';
  if (liveIdle) liveIdle.style.display = 'none';
  const feed = document.getElementById('realtime-feed');
  const d = data.data || data;
  const risk = (d.risk_level || '').toLowerCase();
  const cls = risk === 'high' ? 'risk-high' : risk === 'medium' ? 'risk-med' : '';
  const riskLabel = {'high': '🔴 高风险', 'medium': '🟡 中风险', 'low': '🟢 低风险'}[risk] || '🟢 低风险';
  let reasonsRaw = d.reasons || '';
  if (Array.isArray(reasonsRaw)) {
    reasonsRaw = reasonsRaw.join('、');
  }
  if (!reasonsRaw || reasonsRaw === '未明确说明') {
    reasonsRaw = d.intent || '其他';
  }
  const reasonsArr = (typeof reasonsRaw === 'string' ? reasonsRaw : String(reasonsRaw))
    .split(/[、，,\/]/).filter(function(s) { return s.trim(); });
  const reasonsHtml = reasonsArr.map(function(r) {
    return '<span class="reason-tag">' + r.trim() + '</span>';
  }).join('');
  const existing = document.getElementById('callstart-' + data.call_id);
  if (existing) existing.remove();
  const div = document.createElement('div');
  div.className = 'feed-item ' + cls;
  const progress = (data.index && data.total)
    ? ' <span style="font-size:11px;color:var(--muted)">(' + data.index + '/' + data.total + ')</span>' : '';
  const elapsedStr = data.elapsed_ms != null
    ? '<span style="font-size:11px;color:var(--accent);margin-left:8px">⏱ ' + data.elapsed_ms + 'ms</span>' : '';
  const transcript = d.transcript || '';
  const transcriptHtml = transcript
    ? '<div class="feed-transcript">' + escapeHtml(transcript) + '</div>' : '';
  div.innerHTML = `
    <div class="feed-header">
      <strong>📞 ${data.customer || data.call_id || ''}${progress}${elapsedStr}</strong>
      <span>${data.timestamp ? new Date(data.timestamp).toLocaleTimeString() : ''}</span>
    </div>
    <div style="margin:6px 0">
      <span class="badge ${risk==='high'?'badge-high':risk==='medium'?'badge-med':'badge-low'}">${riskLabel}</span>
      <span style="margin-left:8px;font-size:12px">意图：<strong>${d.intent||''}</strong></span>
      <span style="margin-left:8px;font-size:12px">情绪：${d.sentiment||''}</span>
    </div>
    ${reasonsHtml ? '<div style="margin:4px 0;font-size:12px"><strong style="color:var(--amber)">原因：</strong><span class="reasons-list" style="display:inline-flex">' + reasonsHtml + '</span></div>' : ''}
    <div class="feed-summary">${d.summary||''}</div>
    ${transcriptHtml}
    ${d.suggested_action ? '<div style="font-size:11px;color:var(--accent);margin-top:4px">💡 建议：' + d.suggested_action + '</div>' : ''}
  `;
  feed.insertBefore(div, feed.firstChild);
}

function escapeHtml(text) {
  const d = document.createElement('div');
  d.textContent = text;
  return d.innerHTML;
}

function loadTranscripts() {
  fetch(API + '/api/voice/demo-transcripts').then(r => r.json()).then(d => {
    transcripts = d.transcripts || {};
    const sel = document.getElementById('call-select');
    sel.innerHTML = '<option value="">-- 选择一条通话 --</option>';
    Object.keys(transcripts).forEach(id => {
      const opt = document.createElement('option');
      opt.value = id;
      opt.textContent = id + ' - ' + transcripts[id].substring(0, 30) + '...';
      sel.appendChild(opt);
    });
  }).catch(() => {});
}

function loadTranscript() {
  const id = document.getElementById('call-select').value;
  currentCall = id;
  if (id && transcripts[id]) {
    document.getElementById('transcript-show').textContent = transcripts[id];
  }
}

function analyzeCurrent() {
  const id = document.getElementById('call-select').value;
  if (!id || !transcripts[id]) { toast('请先选择通话', 'error'); return; }
  fetch(API + '/api/voice/analyze', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({transcript: transcripts[id], call_id: id})
  }).then(r => r.json()).then(showResult).catch(() => toast('分析失败', 'error'));
}

function analyzeAll() {
  fetch(API + '/api/voice/demo-analyze-all', {method: 'POST'})
    .then(r => r.json()).then(d => {
      document.getElementById('analysis-result-demo').innerHTML =
        '<div class="result-card header">共分析 <strong>' + d.total + '</strong> 条通话 ('
        + d.completed + ' 完成, ' + d.failed + ' 失败)</div>' +
        (d.results||[]).map(r => renderResultCard(r)).join('');
      loadOverview();
      toast('分析完成，数据已写入 Lance + Iceberg + S3');
    }).catch(() => toast('分析失败', 'error'));
}

function showResult(d) {
  const a = d.analysis || d;
  if (d.timing_ms) {
    a._timing = d.timing_ms;
  }
  if (d.comparison || d.llm_analysis) {
    a._comparison = { keyword: d.comparison?.keyword, llm: d.comparison?.llm || d.llm_analysis };
    a._llm_available = d.llm_available;
  }
  const div = getResultDiv();
  if (div) div.innerHTML = renderResultCard(a);
}

function renderResultCard(d) {
  let reasonsRaw = d.reasons || d.switch_reason || '';
  if (Array.isArray(reasonsRaw)) {
    reasonsRaw = reasonsRaw.join('、');
  }
  if (!reasonsRaw || reasonsRaw === '未明确说明') {
    reasonsRaw = d.intent || '其他';
  }
  const reasonsArr = (typeof reasonsRaw === 'string' ? reasonsRaw : String(reasonsRaw))
    .split(/[、，,\/]/).filter(function(s) { return s.trim(); });
  const reasonsHtml = reasonsArr.map(function(r) {
    return '<span class="reason-tag">' + r.trim() + '</span>';
  }).join('');
  const riskLevel = d.risk_level || 'low';
  const riskBadge = riskLevel === 'high' ? 'badge-high'
    : riskLevel === 'medium' ? 'badge-med' : 'badge-low';
  let timingHtml = '';
  if (d.timing_ms || (d._timing)) {
    const tm = d.timing_ms || d._timing;
    if (tm) {
      timingHtml = '<div style="font-size:10px;color:var(--muted);margin-top:6px;border-top:1px solid #f0f0f0;padding-top:6px">'
        + '⏱ 关键词:' + (tm.llm_keyword || tm.llm_analyze || '?') + 'ms | Lance:'
        + (tm.lance_write || '?') + 'ms | S3:' + (tm.s3_write || '?') + 'ms | 总耗时:'
        + (tm.total || '?') + 'ms</div>';
    }
  }
  let comparisonHtml = '';
  if (d._comparison) {
    const c = d._comparison;
    const kw = c.keyword || {};
    const llm = c.llm || {};
    function vsRow(label, kwVal, llmVal) {
      const kwS = (kwVal != null ? String(kwVal) : '-');
      const llmS = (llmVal != null ? String(llmVal) : '-');
      const match = kwS === llmS;
      return '<tr>'
        + '<td style="padding:3px 6px;font-size:11px;color:var(--muted)">' + label + '</td>'
        + '<td style="padding:3px 6px;font-size:11px;font-weight:500;background:'
        + (match ? '#EAF3DE' : '#FAEEDA') + '">' + kwS + '</td>'
        + '<td style="padding:3px 6px;font-size:11px;font-weight:500;background:'
        + (match ? '#EAF3DE' : '#E6F1FB') + '">' + llmS + '</td>'
        + '<td style="padding:3px 6px;font-size:10px">' + (match ? '✅' : '🔶') + '</td></tr>';
    }
    comparisonHtml = '<div style="margin-top:10px;border-top:1px solid var(--border);padding-top:8px">'
      + '<div style="font-size:12px;font-weight:600;margin-bottom:6px">🔬 关键词 vs LLM 对比</div>'
      + '<table style="width:100%;border-collapse:collapse">'
      + '<tr style="background:#fafafa"><th style="padding:3px 6px;font-size:10px;text-align:left">字段</th>'
      + '<th style="padding:3px 6px;font-size:10px;text-align:left;color:#854F0B">关键词</th>'
      + '<th style="padding:3px 6px;font-size:10px;text-align:left;color:#185FA5">LLM</th>'
      + '<th style="width:24px"></th></tr>'
      + vsRow('意图', kw.intent || d.intent || d.caller_intent, llm.caller_intent)
      + vsRow('原因', kw.reasons || d.reasons || d.switch_reason, llm.switch_reason)
      + vsRow('情绪', kw.sentiment || d.sentiment, llm.sentiment)
      + vsRow('风险', kw.risk || d.risk_level, llm.risk_level)
      + vsRow('建议', d.suggested_action, llm.suggested_action)
      + '</table>';
    if (llm.summary) {
      comparisonHtml += '<div style="margin-top:6px;font-size:11px;line-height:1.5;padding:8px 10px;background:#E6F1FB;border-radius:4px">'
        + '<span style="font-weight:600;color:#185FA5">🤖 LLM 总结：</span>' + llm.summary + '</div>';
    }
    if (d._llm_available) {
      comparisonHtml += '<div style="margin-top:4px;font-size:10px;color:var(--muted);cursor:pointer" '
        + 'onclick="var n=this.nextElementSibling;n.style.display=n.style.display===\'none\'?\'block\':\'none\'">'
        + '📋 点击查看 LLM 完整原始返回 ▼</div>'
        + '<div style="display:none;font-size:10px;font-family:monospace;background:#f5f5f5;padding:8px;border-radius:4px;margin-top:4px;white-space:pre-wrap;max-height:120px;overflow-y:auto">'
        + JSON.stringify(llm, null, 2) + '</div>';
    }
    if (!d._llm_available) {
      comparisonHtml += '<div style="font-size:10px;color:var(--muted);margin-top:4px">💡 设置环境变量 LLM_API_KEY 启用真实 LLM 对比</div>';
    }
    comparisonHtml += '</div>';
  }
  return `
    <div class="result-card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <strong>${d.call_id}</strong>
        <span>
          <span class="badge ${riskBadge}">${riskLevel}风险</span>
          <span class="badge badge-pos" style="margin-left:6px">${d.sentiment||d.intent||''}</span>
          ${d.engine === 'llm' ? '<span class="badge badge-pos" style="margin-left:4px;background:#E6F1FB;color:#185FA5">🤖 LLM</span>' : ''}
        </span>
      </div>
      <div class="result-grid">
        <span class="rl">意图</span><span>${d.intent||d.caller_intent||''}</span>
        <span class="rl">原因</span><div class="reasons-list">${reasonsHtml||'未识别'}</div>
        <span class="rl">建议</span><span>${d.suggested_action||''}</span>
        <span class="rl">总结</span><span style="font-size:12px">${d.summary||''}</span>
      </div>
      ${d.key_entities && Object.keys(d.key_entities).length
        ? '<div style="margin-top:8px;font-size:12px;color:var(--muted)">'
        + (typeof d.key_entities === 'string' ? d.key_entities
        : Object.entries(d.key_entities).map(([k,v]) => k+': '+v).join(' &nbsp;|&nbsp; ')) + '</div>' : ''}
      ${timingHtml}
      ${comparisonHtml}
    </div>
  `;
}

let _simStopped = false;

function startSimulation(mode) {
  mode = mode || 'fixed';
  _simStopped = false;
  document.getElementById('realtime-feed').innerHTML = '';
  const endpoint = mode === 'random' ? '/api/simulation/random-start?count=20' : '/api/simulation/start';
  fetch(API + endpoint, {method: 'POST'})
    .then(r => r.json())
    .then(d => toast((mode === 'random' ? '随机' : '固定') + '模拟已启动，' + d.call_count + ' 条通话排队中'))
    .catch(() => toast('模拟启动失败','error'));
}

function stopSimulation() {
  _simStopped = true;
  // 立即更新 UI 状态
  var liveInd = document.getElementById('live-indicator');
  var liveIdle = document.getElementById('live-idle');
  if (liveInd) liveInd.style.display = 'none';
  if (liveIdle) liveIdle.style.display = '';
  if (liveIdle) liveIdle.textContent = '已停止';

  fetch(API + '/api/simulation/stop', {method: 'POST'})
    .then(r => r.json()).then(() => {
      console.log('[Sim] 模拟已停止');
    }).catch(() => {});

  // 100ms 后恢复文字
  setTimeout(function() {
    var idleEl = document.getElementById('live-idle');
    if (idleEl) idleEl.textContent = '等待中';
  }, 1500);
}

// ========== Benchmark ==========
function runComparison() {
  const div = document.getElementById('comparison-result');
  div.innerHTML = '<div style="text-align:center;padding:30px;color:var(--muted)"><span class="loading-spin"></span> 正在关键词+LLM双路分析中...</div>';

  fetch(API + '/api/benchmark/latency', {method: 'POST'})
    .then(r => r.json())
    .then(d => {
      const t = d.timing_ms;
      const kw = d.comparison?.keyword || d.analysis || {};
      const llm = d.comparison?.llm || d.llm_analysis || {};
      const hasLlm = d.llm_analysis != null && d.llm_analysis.note == null;
      const total = t.total_end_to_end;

      function vsCell(kwVal, llmVal, isFirst) {
        const kwS = kwVal != null ? String(kwVal) : '-';
        const llmS = llmVal != null ? String(llmVal) : '-';
        const match = kwS === llmS;
        const bg = match ? '#EAF3DE' : '#FAEEDA';
        const border = isFirst ? 'border-top:2px solid var(--accent)' : '';
        return '<tr style="' + border + '">'
          + '<td style="padding:6px 10px;font-size:12px;font-weight:500;background:#fafafa">' + kwS + '</td>'
          + '<td style="padding:6px 10px;font-size:12px;background:' + bg + '">' + llmS + '</td>'
          + '<td style="padding:6px 10px;font-size:12px;text-align:center;background:' + bg + '">'
          + (match ? '✅' : '🔶') + '</td></tr>';
      }

      let html = '';
      html += '<div style="background:#fafafa;border-radius:6px;padding:10px;margin-bottom:12px;font-size:11px;max-height:80px;overflow-y:auto;white-space:pre-wrap;line-height:1.5">'
        + '<span style="color:var(--muted);font-size:10px">📝 分析文本 (' + d.transcript_length + '字符):</span><br>'
        + (d.transcript || '').substring(0, 300) + '...</div>';

      html += '<div style="margin-bottom:14px">';
      html += '<div style="display:flex;gap:12px;align-items:flex-end">';
      const kwMs = t.llm_keyword || 0;
      html += '<div style="flex:1;text-align:center">'
        + '<div style="font-size:22px;font-weight:600;color:var(--amber)">' + kwMs.toFixed(1) + ' ms</div>'
        + '<div style="font-size:10px;color:var(--muted)">⚡ 关键词引擎</div></div>';
      html += '<div style="font-size:28px;color:var(--muted)">vs</div>';
      const llmMs = t.llm_real;
      html += '<div style="flex:1;text-align:center">'
        + '<div style="font-size:22px;font-weight:600;color:'
        + (llmMs != null ? (llmMs > 2000 ? 'var(--red)' : 'var(--blue)') : 'var(--muted)') + '">'
        + (llmMs != null ? llmMs.toFixed(0) + ' ms' : 'N/A') + '</div>'
        + '<div style="font-size:10px;color:var(--muted)">🤖 LLM 引擎</div></div>';
      html += '<div style="flex:1;text-align:center">'
        + '<div style="font-size:22px;font-weight:600;color:var(--accent)">' + total.toFixed(0) + ' ms</div>'
        + '<div style="font-size:10px;color:var(--muted)">⏱ 端到端</div></div>';
      html += '</div>';

      if (total > 0) {
        html += '<div style="display:flex;height:6px;border-radius:3px;overflow:hidden;margin-top:8px;background:#eee">';
        html += '<div style="width:' + (kwMs/total*100).toFixed(1) + '%;background:var(--amber);min-width:2px" title="关键词: ' + kwMs.toFixed(1) + 'ms"></div>';
        if (llmMs != null) {
          html += '<div style="width:' + (llmMs/total*100).toFixed(1) + '%;background:var(--blue);min-width:2px" title="LLM: ' + llmMs.toFixed(0) + 'ms"></div>';
        }
        html += '</div>';
        html += '<div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-top:2px">'
          + '<span>关键词</span>' + (llmMs != null ? '<span>LLM</span>' : '') + '<span>其他(I/O)</span></div>';
      }
      html += '</div>';

      html += '<table style="width:100%;border-collapse:collapse;margin-top:10px">';
      html += '<tr style="background:#EEEDFE">'
        + '<th style="padding:8px 10px;font-size:11px;text-align:left;border-radius:6px 0 0 0">字段</th>'
        + '<th style="padding:8px 10px;font-size:11px;text-align:left;color:#854F0B">⚡ 关键词</th>'
        + '<th style="padding:8px 10px;font-size:11px;text-align:left;color:#185FA5">🤖 LLM</th>'
        + '<th style="width:36px;border-radius:0 6px 0 0"></th></tr>';
      const fields = [
        ['意图', kw.intent || d.analysis?.intent, llm.caller_intent || llm.intent],
        ['原因', kw.reasons || d.analysis?.reasons, llm.switch_reason || llm.reasons],
        ['情绪', kw.sentiment || d.analysis?.sentiment, llm.sentiment],
        ['风险', kw.risk || d.analysis?.risk_level, llm.risk_level || llm.risk],
        ['建议', d.analysis?.suggested_action || '-', llm.suggested_action || llm.action],
      ];
      fields.forEach(function(f, i) {
        html += '<tr><td style="padding:6px 10px;font-size:11px;color:var(--muted);font-weight:500;border-bottom:1px solid #f0f0f0">' + f[0] + '</td>';
        html += vsCell(f[1], f[2], i === 0);
        html += '</tr>';
      });
      html += '</table>';

      if (llm.summary) {
        html += '<div style="margin-top:6px;font-size:11px;line-height:1.5;padding:8px 10px;background:#E6F1FB;border-radius:4px">'
          + '<span style="font-weight:600;color:#185FA5">🤖 LLM 总结：</span>' + llm.summary + '</div>';
      }

      if (hasLlm) {
        html += '<div style="margin-top:4px;font-size:10px;color:var(--muted);cursor:pointer" '
          + 'onclick="var n=this.nextElementSibling;n.style.display=n.style.display===\'none\'?\'block\':\'none\'">'
          + '📋 点击查看 LLM 完整原始返回 ▼</div>'
          + '<div style="display:none;font-size:10px;font-family:monospace;background:#f5f5f5;padding:8px;border-radius:4px;margin-top:4px;white-space:pre-wrap;max-height:120px;overflow-y:auto">'
          + JSON.stringify(llm, null, 2) + '</div>';
      }

      const matches = fields.filter(function(f) {
        return f[1] != null && f[2] != null && String(f[1]) === String(f[2]);
      }).length;
      html += '<div style="margin-top:10px;font-size:11px;text-align:center;padding:8px;border-radius:6px;background:'
        + (matches >= 3 ? '#EAF3DE' : matches >= 2 ? '#FAF7F0' : '#FCEBEB') + '">'
        + '一致率: <strong>' + matches + '</strong>/5 '
        + (matches >= 4 ? '🎉 高度一致' : matches >= 2 ? '🔶 部分差异' : '⚠️ 差异较大')
        + '</div>';

      if (!hasLlm) {
        html += '<div style="font-size:11px;color:var(--muted);text-align:center;margin-top:8px;padding:8px;background:#fafafa;border-radius:4px">'
          + '💡 设置 <code>LLM_API_KEY</code> 环境变量后重启，可看到真实 LLM 结果</div>';
      }

      div.innerHTML = html;
    })
    .catch(function(e) {
      div.innerHTML = '<div style="padding:12px;color:var(--red)">对比失败: ' + e.message + '</div>';
    });
}

function runLatencyBenchmark() {
  const div = document.getElementById('latency-result');
  div.innerHTML = '<div style="text-align:center;padding:20px;color:var(--muted)"><span class="loading-spin"></span> 正在测量端到端延迟...</div>';

  fetch(API + '/api/benchmark/latency', {method: 'POST'})
    .then(r => r.json())
    .then(d => {
      const t = d.timing_ms;
      const total = t.total_end_to_end;
      const llmKwMs = t.llm_keyword || t.llm_analyze || 0;
      const llmPct = total > 0 ? (llmKwMs / total * 100).toFixed(1) : 0;
      const lancePct = total > 0 ? (t.lance_write / total * 100).toFixed(1) : 0;
      const s3Pct = total > 0 ? (t.s3_write / total * 100).toFixed(1) : 0;

      function barColor(val, threshold) {
        return val < threshold ? 'var(--green)'
          : (val < threshold * 3 ? 'var(--amber)' : 'var(--red)');
      }
      function barWidth(val) { return Math.min(100, Math.max(1, val / total * 100)); }

      let html = '<div style="margin-bottom:16px">';
      html += '<div style="font-size:13px;margin-bottom:8px;"><strong>端到端总耗时: '
        + t.total_end_to_end.toFixed(1) + ' ms</strong> (文本长度: '
        + d.transcript_length + ' 字符)</div>';

      html += '<div style="margin-bottom:8px">';
      html += '<div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:2px"><span>关键词解析</span><span><strong>'
        + llmKwMs.toFixed(3) + ' ms</strong> (' + llmPct + '%)</span></div>';
      html += '<div style="background:#eee;border-radius:3px;height:10px"><div style="background:'
        + barColor(llmKwMs, 50) + ';width:' + barWidth(llmKwMs)
        + '%;height:10px;border-radius:3px;min-width:2px"></div></div>';
      html += '</div>';

      if (t.llm_real != null) {
        const llmRealPct = total > 0 ? (t.llm_real / total * 100).toFixed(1) : 0;
        html += '<div style="margin-bottom:8px">';
        html += '<div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:2px"><span>🤖 LLM 解析</span><span><strong>'
          + t.llm_real.toFixed(0) + ' ms</strong> (' + llmRealPct + '%)</span></div>';
        html += '<div style="background:#eee;border-radius:3px;height:10px"><div style="background:var(--blue);width:'
          + barWidth(t.llm_real) + '%;height:10px;border-radius:3px;min-width:2px"></div></div>';
        html += '</div>';
      }

      html += '<div style="margin-bottom:8px">';
      html += '<div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:2px"><span>Lance 写入</span><span><strong>'
        + t.lance_write.toFixed(3) + ' ms</strong> (' + lancePct + '%)</span></div>';
      html += '<div style="background:#eee;border-radius:3px;height:10px"><div style="background:'
        + barColor(t.lance_write, 100) + ';width:' + barWidth(t.lance_write)
        + '%;height:10px;border-radius:3px;min-width:2px"></div></div>';
      html += '</div>';

      html += '<div style="margin-bottom:8px">';
      html += '<div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:2px"><span>S3 写入</span><span><strong>'
        + t.s3_write.toFixed(3) + ' ms</strong> (' + s3Pct + '%)</span></div>';
      html += '<div style="background:#eee;border-radius:3px;height:10px"><div style="background:'
        + barColor(t.s3_write, 200) + ';width:' + barWidth(t.s3_write)
        + '%;height:10px;border-radius:3px;min-width:2px"></div></div>';
      html += '</div>';

      html += '<div style="margin-bottom:4px">';
      html += '<div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:2px"><span>总耗时</span><span><strong>'
        + t.total_end_to_end.toFixed(3) + ' ms</strong></span></div>';
      html += '<div style="background:#eee;border-radius:3px;height:12px"><div style="background:var(--accent);width:100%;height:12px;border-radius:3px"></div></div>';
      html += '</div>';
      html += '</div>';

      html += '<div style="background:#fafafa;border-radius:6px;padding:10px;font-size:12px">';
      html += '<strong>分析结果:</strong> ';
      html += '意图=<span class="badge badge-med">' + (d.analysis?.intent || '?') + '</span> ';
      html += '原因=' + (d.analysis?.reasons || '?') + ' ';
      html += '情绪=<span class="badge ' + (d.analysis?.sentiment === 'negative' ? 'badge-high' : 'badge-low') + '">' + (d.analysis?.sentiment || '?') + '</span> ';
      html += '风险=<span class="badge badge-high">' + (d.analysis?.risk_level || '?') + '</span>';
      html += '</div>';

      if (d.comparison && d.llm_analysis) {
        const kw = d.comparison.keyword;
        const llm = d.llm_analysis;
        html += '<div style="margin-top:8px;background:#fafafa;border-radius:6px;padding:10px;font-size:11px">';
        html += '<strong>🔬 关键词 vs LLM 对比:</strong>';
        html += '<table style="width:100%;margin-top:4px;font-size:10px;border-collapse:collapse">';
        html += '<tr style="background:#fff"><td style="padding:2px 4px">意图</td><td style="color:#854F0B">' + (kw.intent || '-') + '</td><td style="color:#185FA5">' + (llm.caller_intent || '-') + '</td></tr>';
        html += '<tr><td style="padding:2px 4px">原因</td><td style="color:#854F0B">' + (kw.reasons || '-') + '</td><td style="color:#185FA5">' + (llm.switch_reason || '-') + '</td></tr>';
        html += '<tr style="background:#fff"><td style="padding:2px 4px">情绪</td><td style="color:#854F0B">' + (kw.sentiment || '-') + '</td><td style="color:#185FA5">' + (llm.sentiment || '-') + '</td></tr>';
        html += '<tr><td style="padding:2px 4px">风险</td><td style="color:#854F0B">' + (kw.risk || '-') + '</td><td style="color:#185FA5">' + (llm.risk_level || '-') + '</td></tr>';
        html += '</table></div>';
      } else {
        html += '<div style="font-size:10px;color:var(--muted);margin-top:4px">💡 设置 LLM_API_KEY 环境变量启用 LLM 对比</div>';
      }

      div.innerHTML = html;
    })
    .catch(e => {
      div.innerHTML = '<div style="padding:12px;color:var(--red)">测量失败: ' + e.message + '</div>';
    });
}

function runThroughput() {
  const div = document.getElementById('throughput-result');
  div.innerHTML = '<div style="text-align:center;padding:20px;color:var(--muted)"><span class="loading-spin"></span> 正在执行吞吐量测试...</div>';

  fetch(API + '/api/benchmark/concurrency?concurrency=10&total_requests=30', {method: 'POST'})
    .then(r => r.json())
    .then(d => {
      const l = d.latency_ms;
      let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">';
      html += '<div style="background:#f8f7f4;border-radius:8px;padding:14px">';
      html += '<div style="font-size:13px;font-weight:600;margin-bottom:8px">📊 概览</div>';
      html += '<div style="font-size:12px;line-height:2">';
      html += '<div>总请求: <strong>' + d.config.total_requests + '</strong></div>';
      html += '<div>并发数: <strong>' + d.config.concurrency + '</strong></div>';
      html += '<div>错误数: <strong style="color:'
        + (d.errors > 0 ? 'var(--red)' : 'var(--green)') + '">' + d.errors + '</strong></div>';
      html += '<div>总耗时: <strong>' + d.timing_ms.total_wall_time + ' ms</strong></div>';
      html += '</div></div>';

      html += '<div style="background:#f8f7f4;border-radius:8px;padding:14px">';
      html += '<div style="font-size:13px;font-weight:600;margin-bottom:8px">🚀 吞吐</div>';
      html += '<div style="text-align:center">';
      html += '<div style="font-size:32px;font-weight:600;color:'
        + (d.timing_ms.throughput_qps > 50 ? 'var(--green)' : 'var(--amber)') + '">'
        + d.timing_ms.throughput_qps + '</div>';
      html += '<div style="font-size:12px;color:var(--muted)">QPS (请求/秒)</div>';
      html += '</div></div>';
      html += '</div>';

      html += '<div style="background:#f8f7f4;border-radius:8px;padding:14px;margin-bottom:16px">';
      html += '<div style="font-size:13px;font-weight:600;margin-bottom:10px">📈 延迟分布 (ms)</div>';
      html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px">';
      [
        ['Min', l.min, 50], ['P50', l.p50, 100], ['P75', l.p75, 200], ['P95', l.p95, 500],
        ['P99', l.p99, 1000], ['Max', l.max, 2000], ['Avg', l.avg, 300], ['StdDev', l.stdev, 100]
      ].forEach(([label, val, threshold]) => {
        const color = val < threshold ? 'var(--green)'
          : (val < threshold * 3 ? 'var(--amber)' : 'var(--red)');
        html += '<div style="text-align:center;background:#fff;border-radius:6px;padding:8px;border:1px solid #e8e8e6">';
        html += '<div style="font-size:10px;color:var(--muted);margin-bottom:4px">' + label + '</div>';
        html += '<div style="font-size:18px;font-weight:600;color:' + color + '">'
          + (typeof val === 'number' ? val.toFixed(1) : val) + '</div>';
        html += '</div>';
      });
      html += '</div></div>';

      div.innerHTML = html;
      toast('吞吐量测试完成！QPS: ' + d.timing_ms.throughput_qps);
    })
    .catch(e => {
      div.innerHTML = '<div style="padding:12px;color:var(--red)">测试失败: ' + e.message + '</div>';
    });
}

// ========== Data Management ==========
function loadStorageData() {
  fetch(API + '/api/lance/stats').then(r => r.json()).then(d => {
    const intents = d.intent_distribution || {};
    const intentStr = Object.entries(intents).map(([k,v]) => k + ': ' + v).join(', ') || '0';
    document.getElementById('lance-data').innerHTML =
      '<div style="margin-bottom:12px"><strong>voice_analysis.lance</strong> <span style="color:var(--muted);font-size:11px">Lance Format</span>'
      + '<div style="font-size:12px;margin-top:4px">记录数: <strong>' + d.records + '</strong> | 版本: v' + (d.schema_version||'?') + '</div>'
      + '<div style="font-size:12px;color:var(--muted)">意图分布: ' + intentStr + '</div>'
      + '<div style="font-size:12px;color:var(--muted)">风险分布: high=' + (d.risk_distribution?.high||0)
      + ', medium=' + (d.risk_distribution?.medium||0) + ', low=' + (d.risk_distribution?.low||0) + '</div></div>';
  }).catch(() => {
    document.getElementById('lance-data').innerHTML = '<span style="color:var(--red)">Lance 不可用</span>';
  });

  fetch(API + '/api/iceberg/stats').then(r => r.json()).then(d => {
    const rd = d.risk_distribution || {};
    document.getElementById('iceberg-data').innerHTML =
      '<div style="margin-bottom:12px"><strong>churn_risk.churn_predictions</strong> <span style="color:var(--muted);font-size:11px">Apache Iceberg</span>'
      + '<div style="font-size:12px;margin-top:4px">记录数: <strong>' + d.records + '</strong> | 快照: ' + d.snapshots + '</div>'
      + '<div style="font-size:12px;color:var(--muted)">风险: high=' + (rd.high||0) + ', medium=' + (rd.medium||0) + ', low=' + (rd.low||0) + '</div>'
      + '<div style="font-size:11px;color:var(--muted);margin-top:2px">特性: ' + (d.features||[]).join(' | ') + '</div></div>';
  }).catch(() => {
    document.getElementById('iceberg-data').innerHTML = '<span style="color:var(--red)">Iceberg 不可用</span>';
  });

  loadImageLanceData();
}

function loadImageLanceData() {
  var div = document.getElementById('image-lance-data');
  if (!div) return;
  div.innerHTML = '<div style="padding:20px;text-align:center;color:var(--muted)">正在读取 images.lance...</div>';
  imageRequest('/api/image/records?limit=100').then(function(data) {
    var rows = data.records || [];
    var summary = data.summary || {};
    var html = '<div style="font-size:12px;color:var(--muted);margin-bottom:10px">'
      + '<strong style="color:var(--text)">' + escapeHtml(data.dataset || 'images.lance') + '</strong>'
      + ' · ' + rows.length + ' / ' + (data.count || 0) + ' 条'
      + ' · 已分析 ' + (summary.analyzed || 0)
      + ' · 已生成向量 ' + (summary.embedded || 0)
      + '</div>'
      + '<div class="image-summary-row">'
      + '<div><strong>' + (data.count || 0) + '</strong><span>表记录数</span></div>'
      + '<div><strong style="color:var(--green)">' + (summary.avatars || 0) + '</strong><span>合规头像</span></div>'
      + '<div><strong style="color:var(--amber)">' + (summary.rejected || 0) + '</strong><span>不合规</span></div>'
      + '<div><strong style="color:var(--red)">' + (summary.failed || 0) + '</strong><span>处理失败</span></div>'
      + '</div>';
    if (!rows.length) {
      html += '<div style="padding:20px;text-align:center;color:var(--muted)">表中暂无图片记录，请先运行图片流水线</div>';
    } else {
      html += '<div class="image-results-grid">';
      rows.forEach(function(row) { html += renderImageCard(row); });
      html += '</div>';
    }
    div.innerHTML = html;
  }).catch(function(error) {
    div.innerHTML = '<div class="image-error">暂时无法读取图片 Lance 表：'
      + escapeHtml(error.message) + '</div>';
  });
}

// ========== Similarity Search ==========
function runSimilaritySearch() {
  const q = document.getElementById('similarity-query').value.trim();
  if (!q) { toast('请输入搜索文本'); return; }
  const resDiv = document.getElementById('similarity-result');
  resDiv.innerHTML = '<div style="text-align:center;padding:20px;color:var(--muted)">🔍 正在向量相似度搜索...</div>';

  fetch(API + '/api/lance/search', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({query: q, top_k: 5})
  }).then(r => r.json()).then(d => {
    if (!d.results || d.results.length === 0) {
      resDiv.innerHTML = '<div style="padding:16px;color:var(--muted)">未找到相似结果，请先运行语音分析生成数据</div>';
    } else {
      let html = '<div style="font-size:12px;margin-bottom:8px;color:var(--muted)">找到 <strong>'
        + d.results.length + '</strong> 条相似记录（基于向量语义相似度）</div>';
      d.results.forEach((r, i) => {
        let riskBadge = r.risk_level === 'high' ? 'badge-high'
          : (r.risk_level === 'medium' ? 'badge-med' : 'badge-low');
        let sentBadge = r.sentiment === 'negative' ? 'badge-neg'
          : (r.sentiment === 'positive' ? 'badge-pos' : 'badge-med');
        html += '<div style="display:flex;gap:10px;padding:10px;margin-bottom:6px;background:#fafafa;border-radius:6px;border-left:3px solid '
          + (r.risk_level === 'high' ? 'var(--red)' : r.risk_level === 'medium' ? 'var(--amber)' : 'var(--green)') + '">'
          + '<div style="flex:1;min-width:0">'
          + '<div style="font-size:12px;font-weight:600;margin-bottom:3px">' + (i+1) + '. 📞 ' + (r.call_id || '?') + '</div>'
          + '<div style="font-size:11px;color:var(--text);margin-bottom:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
          + (r.transcript || '').substring(0,100) + '...</div>'
          + '<div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:4px">'
          + '<span class="badge ' + riskBadge + '" style="font-size:10px">🎯 ' + (r.caller_intent || '?') + '</span>'
          + '<span class="badge ' + riskBadge + '" style="font-size:10px">⚠️ ' + (r.risk_level || '?') + '</span>'
          + '<span class="badge ' + sentBadge + '" style="font-size:10px">😊 ' + (r.sentiment || '?') + '</span>'
          + '<span class="badge" style="font-size:10px;background:#FFF3E0;color:#E65100">💡 ' + (r.suggested_action || 'N/A') + '</span>'
          + (r._similarity != null ? '<span class="badge" style="font-size:10px;background:#E6F1FB;color:#185FA5">📏 相似度 ' + (r._similarity * 100).toFixed(1) + '%</span>' : '')
          + '</div>';
        if (r.switch_reason) {
          let reasons = Array.isArray(r.switch_reason) ? r.switch_reason : [r.switch_reason];
          html += '<div class="reasons-list" style="margin-top:4px">'
            + reasons.map(x => '<span class="reason-tag">' + x + '</span>').join('') + '</div>';
        }
        html += '</div></div>';
      });
      resDiv.innerHTML = html;
    }
  }).catch(e => {
    resDiv.innerHTML = '<div style="padding:12px;color:var(--red)">搜索失败: ' + e.message + '</div>';
  });
}

// Enter key triggers search
document.addEventListener('DOMContentLoaded', () => {
  const input = document.getElementById('similarity-query');
  if (input) input.addEventListener('keydown', e => { if (e.key === 'Enter') runSimilaritySearch(); });
});

// ========== History Data Browser ==========
function loadHistoryData() {
  const div = document.getElementById('history-list');
  div.innerHTML = '<span style="color:var(--muted)">加载中...</span>';
  fetch(API + '/api/lance/records?dataset=audio&limit=20').then(r => r.json()).then(d => {
    if (!d.records || d.records.length === 0) {
      div.innerHTML = '<div style="padding:20px;color:var(--muted);text-align:center">暂无历史数据，请先运行语音分析</div>';
      return;
    }
    let html = '<div style="font-size:11px;color:var(--muted);margin-bottom:6px">显示 '
      + d.records.length + ' / ' + d.count + ' 条记录</div>';
    d.records.forEach(r => {
      let riskBadge = r.risk_level === 'high' ? 'badge-high'
        : (r.risk_level === 'medium' ? 'badge-med' : 'badge-low');
      let sentBadge = r.sentiment === 'negative' ? 'badge-neg'
        : (r.sentiment === 'positive' ? 'badge-pos' : 'badge-med');
      html += '<div style="display:flex;gap:8px;padding:8px 10px;margin-bottom:3px;background:#fafafa;border-radius:4px;align-items:center">'
        + '<span style="font-size:11px;font-weight:600;color:var(--accent);min-width:70px">' + (r.call_id || '?') + '</span>'
        + '<span style="font-size:11px;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
        + (r.transcript || '').substring(0,60) + '...</span>'
        + '<span class="badge ' + riskBadge + '" style="font-size:9px">' + (r.caller_intent || '?') + '</span>'
        + '<span class="badge ' + riskBadge + '" style="font-size:9px">' + (r.risk_level || '?') + '</span>'
        + '<span class="badge ' + sentBadge + '" style="font-size:9px">' + (r.sentiment || '?') + '</span>'
        + '</div>';
    });
    div.innerHTML = html;
  }).catch(e => { div.innerHTML = '<span style="color:var(--red)">加载失败: ' + e.message + '</span>'; });
}

function loadAllHistory() {
  const div = document.getElementById('history-list');
  div.innerHTML = '<span style="color:var(--muted)">加载全部记录...</span>';
  fetch(API + '/api/lance/records?dataset=audio&limit=200').then(r => r.json()).then(d => {
    if (!d.records || d.records.length === 0) {
      div.innerHTML = '<div style="padding:20px;color:var(--muted);text-align:center">暂无历史数据</div>';
      return;
    }
    let html = '<div style="font-size:11px;color:var(--muted);margin-bottom:6px">全部 ' + d.count + ' 条记录</div>';
    d.records.forEach(r => {
      let riskBadge = r.risk_level === 'high' ? 'badge-high'
        : (r.risk_level === 'medium' ? 'badge-med' : 'badge-low');
      let sentBadge = r.sentiment === 'negative' ? 'badge-neg'
        : (r.sentiment === 'positive' ? 'badge-pos' : 'badge-med');
      html += '<div style="display:flex;gap:8px;padding:6px 8px;margin-bottom:2px;background:#fafafa;border-radius:4px;align-items:center;font-size:11px">'
        + '<span style="font-weight:600;color:var(--accent);min-width:70px">' + (r.call_id || '?') + '</span>'
        + '<span style="flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'
        + (r.transcript || '').substring(0,50) + '...</span>'
        + '<span class="badge ' + riskBadge + '" style="font-size:9px">' + (r.caller_intent || '?') + '</span>'
        + '<span class="badge ' + riskBadge + '" style="font-size:9px">' + (r.risk_level || '?') + '</span>'
        + '<span class="badge ' + sentBadge + '" style="font-size:9px">' + (r.sentiment || '?') + '</span>'
        + '</div>';
    });
    div.innerHTML = html;
  }).catch(e => { div.innerHTML = '<span style="color:var(--red)">加载失败: ' + e.message + '</span>'; });
}

// ========== SQL Query ==========
function loadSqlTables() {
  fetch(API + '/api/data/sql/tables')
    .then(r => r.json())
    .then(d => {
      document.getElementById('sql-tables-list').textContent =
        (d.tables || []).join(', ') || '(无表)';
    })
    .catch(() => {
      document.getElementById('sql-tables-list').textContent = '查询失败';
    });
}

function refreshSqlTables() {
  const listEl = document.getElementById('sql-tables-list');
  listEl.textContent = '刷新中...';
  fetch(API + '/api/data/sql/refresh', { method: 'POST' })
    .then(r => r.json())
    .then(d => {
      listEl.textContent = (d.tables || []).join(', ') || '(无表)';
      document.getElementById('sql-result').innerHTML =
        '<div style="font-size:11px;color:var(--green);margin-top:8px">数据源已刷新</div>';
    })
    .catch(() => { listEl.textContent = '刷新失败'; });
}

function executeSqlQuery() {
  const sql = document.getElementById('sql-editor').value.trim();
  if (!sql) { toast('请输入 SQL 语句'); return; }
  const resDiv = document.getElementById('sql-result');
  resDiv.innerHTML = '<span style="color:var(--muted);font-size:12px">执行中...</span>';

  fetch(API + '/api/data/sql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql: sql })
  }).then(r => r.json()).then(d => {
    if (!d.success) {
      resDiv.innerHTML = '<div class="sql-error">' + escapeHtml(d.error || '未知错误') + '</div>';
      return;
    }
    let html = '<div class="sql-meta">';
    html += '返回 ' + d.row_count + ' 行';
    if (d.truncated) html += ' <span style="color:var(--amber)">(结果已截断，上限500行)</span>';
    html += ' | 耗时 ' + d.elapsed_ms + 'ms';
    html += '</div>';
    if (d.rows.length === 0) {
      html += '<div style="padding:16px;color:var(--muted);text-align:center">查询无结果</div>';
    } else {
      html += '<div style="overflow-x:auto"><table class="sql-result-table"><tr>';
      d.columns.forEach(function(c) {
        html += '<th>' + escapeHtml(c) + '</th>';
      });
      html += '</tr>';
      d.rows.forEach(function(row) {
        html += '<tr>';
        d.columns.forEach(function(c) {
          var val = row[c];
          if (val === null || val === undefined) val = '<i style="color:var(--muted)">NULL</i>';
          var display = String(val);
          if (display.length > 80) display = display.substring(0, 80) + '...';
          html += '<td title="' + escapeHtml(String(val)) + '">' + escapeHtml(display) + '</td>';
        });
        html += '</tr>';
      });
      html += '</table></div>';
    }
    resDiv.innerHTML = html;
  }).catch(function(e) {
    resDiv.innerHTML = '<div class="sql-error">请求失败: ' + escapeHtml(e.message) + '</div>';
  });
}

function quickSql(type) {
  var sqls = {
    stats: 'SELECT risk_level, COUNT(*) AS cnt FROM voice_analysis GROUP BY risk_level ORDER BY cnt DESC',
    churn: 'SELECT caller_intent, COUNT(*) AS cnt FROM voice_analysis WHERE caller_intent LIKE \'%转网%\' OR caller_intent LIKE \'%销户%\' GROUP BY caller_intent ORDER BY cnt DESC',
    sentiment: 'SELECT sentiment, COUNT(*) AS cnt, AVG(sentiment_score) AS avg_score FROM voice_analysis GROUP BY sentiment ORDER BY cnt DESC',
    iceberg: 'SELECT * FROM churn_predictions ORDER BY date DESC'
  };
  var sql = sqls[type] || '';
  document.getElementById('sql-editor').value = sql;
  localStorage.setItem('sql_editor_text', sql);
  executeSqlQuery();
}

// Enter key triggers SQL execution
document.addEventListener('keydown', function(e) {
  if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
    var sqlEditor = document.getElementById('sql-editor');
    if (sqlEditor && document.activeElement === sqlEditor) {
      e.preventDefault();
      executeSqlQuery();
    }
  }
});

function loadOverview() {
  fetch(API + '/api/overview').then(r => r.json()).then(d => {
    document.getElementById('hs-lance').textContent = (d.lance?.records || 0) + ' rows';
    document.getElementById('hs-ice').textContent = (d.iceberg?.records || 0) + ' rows';
    document.getElementById('hs-s3').textContent = (d.s3?.total_objects || 0) + ' objs';
  }).catch(() => {});
}

// ========== Tab switching ==========
let activeAudioTab = 'offline';

function showDemoWorkspace(workspace) {
  const isImage = workspace === 'image';
  const audioHeader = document.querySelector('.audio-workspace-header');
  const audioTabs = document.querySelector('.demo-tabs');
  if (audioHeader) audioHeader.style.display = isImage ? 'none' : '';
  if (audioTabs) audioTabs.style.display = isImage ? 'none' : '';
  document.querySelectorAll('.dt-content').forEach(content => { content.style.display = 'none'; });

  if (isImage) {
    const imageContent = document.getElementById('dt-image');
    if (imageContent) imageContent.style.display = '';
    loadImageStatus();
    return;
  }

  document.querySelectorAll('.demo-tab').forEach(tab => {
    tab.classList.toggle('active', tab.dataset.dtab === activeAudioTab);
  });
  const audioContent = document.getElementById('dt-' + activeAudioTab);
  if (audioContent) audioContent.style.display = '';
}

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    const panel = document.getElementById('panel-' + tab.dataset.panel);
    if (panel) panel.classList.add('active');
    if (tab.dataset.panel === 'demo') showDemoWorkspace(tab.dataset.workspace || 'audio');
    if (tab.dataset.panel === 'data') { loadStorageData(); loadSqlTables(); }
    if (tab.dataset.panel === 'verify') runVerify();
  });
});

// 音频工作区内部切换（批量 / 实时 / 录制 / 文字）
document.querySelectorAll('.demo-tab').forEach(dtab => {
  dtab.addEventListener('click', () => {
    activeAudioTab = dtab.dataset.dtab;
    document.querySelectorAll('.demo-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.dt-content').forEach(c => c.style.display = 'none');
    dtab.classList.add('active');
    const content = document.getElementById('dt-' + dtab.dataset.dtab);
    if (content) content.style.display = '';
  });
});

function getResultDiv() {
  const activeDTab = document.querySelector('.demo-tab.active');
  if (activeDTab && activeDTab.dataset.dtab === 'voice') {
    return document.getElementById('analysis-result');
  }
  // Switch to text sub-tab before showing results
  if (activeDTab && activeDTab.dataset.dtab !== 'text') {
    activeAudioTab = 'text';
    document.querySelectorAll('.demo-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.dt-content').forEach(c => c.style.display = 'none');
    const textTab = document.querySelector('.demo-tab[data-dtab="text"]');
    if (textTab) textTab.classList.add('active');
    const textContent = document.getElementById('dt-text');
    if (textContent) textContent.style.display = '';
  }
  return document.getElementById('analysis-result-demo');
}

// ========== Audio Recording ==========
let mediaRecorder = null;
let audioChunks = [];
let audioBlob = null;
let recTimer = null;
let recSeconds = 0;
let hasRecording = false;

function toggleRecording() {
  if (mediaRecorder && mediaRecorder.state === 'recording') {
    stopRecording();
  } else {
    startRecording();
  }
}

async function startRecording() {
  audioChunks = [];
  hasRecording = false;
  const txEl = document.getElementById('rec-transcript');
  txEl.innerText = '';
  document.getElementById('btn-analyze-rec').disabled = true;
  document.getElementById('btn-clear-rec').disabled = true;
  document.getElementById('audio-player').style.display = 'none';

  try {
    const stream = await navigator.mediaDevices.getUserMedia({audio: true});
    const opts = {};
    if (MediaRecorder.isTypeSupported('audio/webm;codecs=opus')) {
      opts.mimeType = 'audio/webm;codecs=opus';
    }
    mediaRecorder = new MediaRecorder(stream, opts);
    mediaRecorder.ondataavailable = e => { if (e.data.size > 0) audioChunks.push(e.data); };
    mediaRecorder.onstop = () => {
      audioBlob = new Blob(audioChunks, {type: 'audio/webm'});
      const url = URL.createObjectURL(audioBlob);
      const player = document.getElementById('audio-player');
      player.src = url;
      player.style.display = '';
      hasRecording = true;
      document.getElementById('btn-analyze-rec').disabled = false;
      document.getElementById('btn-clear-rec').disabled = false;
      const tx = document.getElementById('rec-transcript');
      if (!tx.innerText.trim()) {
        tx.innerText = '';
        if (tx.innerText === '') tx.setAttribute('data-placeholder',
          '录音已完成！请在此输入或粘贴通话内容，然后点击「分析这段语音」');
      }
      stopWaveform();
      stream.getTracks().forEach(t => t.stop());
    };
    mediaRecorder.start(500);
    const btn = document.getElementById('record-btn');
    btn.classList.add('recording');
    btn.textContent = '⏹';
    btn.title = '点击停止录音';
    recSeconds = 0;
    document.getElementById('rec-timer').style.display = '';
    document.getElementById('rec-timer').textContent = '00:00';
    recTimer = setInterval(() => {
      recSeconds++;
      const m = String(Math.floor(recSeconds/60)).padStart(2,'0');
      const s = String(recSeconds%60).padStart(2,'0');
      document.getElementById('rec-timer').textContent = m + ':' + s;
    }, 1000);
    startWaveform();
  } catch(e) {
    toast('无法访问麦克风: ' + e.message, 'error');
  }
}

function stopRecording() {
  if (mediaRecorder && mediaRecorder.state === 'recording') {
    mediaRecorder.stop();
  }
  if (recTimer) { clearInterval(recTimer); recTimer = null; }
  const btn = document.getElementById('record-btn');
  btn.classList.remove('recording');
  btn.textContent = '🎤';
  btn.title = '点击开始录音';
}

function clearRecording() {
  audioBlob = null;
  hasRecording = false;
  document.getElementById('audio-player').style.display = 'none';
  const tx = document.getElementById('rec-transcript');
  tx.innerText = '';
  tx.setAttribute('data-placeholder', '点击🎤开始录音，或在此直接输入通话内容...');
  document.getElementById('rec-timer').style.display = 'none';
  document.getElementById('btn-analyze-rec').disabled = true;
  document.getElementById('btn-clear-rec').disabled = true;
  document.getElementById('analysis-result').innerHTML =
    '<div style="color:var(--muted);font-size:13px;padding:20px;text-align:center">录制或上传语音后，输入通话内容并点击分析</div>';
}

// Waveform animation
let waveformInterval = null;
function startWaveform() {
  const container = document.getElementById('waveform');
  container.innerHTML = '';
  for (let i = 0; i < 24; i++) {
    const bar = document.createElement('div');
    bar.className = 'bar';
    bar.style.height = '4px';
    container.appendChild(bar);
  }
  waveformInterval = setInterval(() => {
    document.querySelectorAll('#waveform .bar').forEach(b => {
      b.style.height = Math.floor(Math.random() * 36 + 4) + 'px';
    });
  }, 100);
}
function stopWaveform() {
  if (waveformInterval) { clearInterval(waveformInterval); waveformInterval = null; }
  document.getElementById('waveform').innerHTML = '';
}

function analyzeRecording() {
  const txEl = document.getElementById('rec-transcript');
  const text = txEl.innerText.trim();
  if (!text) { toast('请先输入通话内容（录完音后在此输入或粘贴）', 'error'); return; }
  const callId = 'rec_' + new Date().toISOString().replace(/[:.]/g,'-');
  fetch(API + '/api/voice/analyze', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({transcript: text, call_id: callId})
  }).then(r => r.json()).then(d => {
    const resultDiv = document.getElementById('analysis-result');
    resultDiv.innerHTML = renderResultCard(d.analysis || d);
    loadOverview();
    toast('语音分析完成！');
  }).catch(() => toast('分析失败', 'error'));
}

// File upload
function handleFileUpload(event) {
  const file = event.target.files[0];
  if (!file) return;
  if (!file.type.startsWith('audio/')) { toast('请选择音频文件', 'error'); return; }
  const url = URL.createObjectURL(file);
  const player = document.getElementById('audio-player');
  player.src = url;
  player.style.display = '';
  audioBlob = file;
  hasRecording = true;
  const tx = document.getElementById('rec-transcript');
  tx.innerText = '';
  tx.setAttribute('data-placeholder',
    '已加载音频文件: ' + file.name + '，请在此输入或粘贴通话内容，然后点击「分析这段语音」');
  document.getElementById('btn-analyze-rec').disabled = false;
  document.getElementById('btn-clear-rec').disabled = false;
  document.getElementById('record-btn').classList.remove('recording');
  document.getElementById('record-btn').textContent = '🎤';
  document.getElementById('rec-timer').style.display = 'none';
  if (recTimer) { clearInterval(recTimer); recTimer = null; }
  stopWaveform();
  const zone = document.getElementById('upload-zone');
  zone.classList.add('has-file');
  document.getElementById('upload-hint').textContent = '已选择: ' + file.name;
}

// Placeholder behavior for contenteditable
document.addEventListener('focusin', function(e) {
  if (e.target.id === 'rec-transcript') {
    e.target.classList.add('focus');
  }
});
document.addEventListener('focusout', function(e) {
  if (e.target.id === 'rec-transcript') {
    e.target.classList.remove('focus');
  }
});

// ========== Verification Center ==========
let verifyData = null;

function runVerify() {
  const grid = document.getElementById('verify-grid');
  const summary = document.getElementById('verify-summary');
  const errBox = document.getElementById('verify-error');
  grid.innerHTML = '<div style="color:var(--muted);font-size:13px;padding:20px;text-align:center;grid-column:1/-1"><span class="loading-spin"></span>正在探测所有组件...</div>';
  summary.innerHTML = '<span class="loading-spin"></span>正在验证所有组件...';
  errBox.style.display = 'none';

  fetch(API + '/api/system/verify')
    .then(r => r.json())
    .then(d => {
      verifyData = d;
      renderVerifyResult(d);
    })
    .catch(e => {
      errBox.style.display = '';
      document.getElementById('verify-error-detail').textContent = '验证服务异常: ' + e.message;
      summary.innerHTML = '<span class="count err">✗</span><div><div class="label">验证失败</div><div style="font-size:11px;color:var(--muted)">请检查后台服务</div></div>';
    });
}

function renderVerifyResult(d) {
  const grid = document.getElementById('verify-grid');
  const summary = document.getElementById('verify-summary');
  const s = d.summary || {};

  const allOk = s.all_online;
  summary.innerHTML =
    '<span class="count ' + (allOk ? 'ok' : 'err') + '">' + (allOk ? '✓' : '✗') + '</span>' +
    '<div><div class="label">组件状态</div><div style="font-size:11px;color:var(--muted)">' +
    s.online + '/' + s.total + ' 在线 (' + (d.version || '') + ')</div></div>' +
    (allOk
      ? '<span class="badge badge-low" style="margin-left:auto">全部正常</span>'
      : '<span class="badge badge-high" style="margin-left:auto">' + s.offline + ' 离线</span>');

  grid.innerHTML = (d.components || []).map(c => renderVerifyCard(c)).join('');
}

function renderVerifyCard(c) {
  const hasMgmt = c.management_url != null;
  const mgmtBtn = hasMgmt
    ? '<a class="btn-ext" href="' + c.management_url + '" target="_blank" title="在新窗口打开管理界面">🔗 ' + (c.management_label || '管理界面') + '</a>'
    : '<span class="btn-ext" style="border-color:var(--border);color:var(--muted);cursor:default">📊 ' + (c.management_label || 'API 查询') + '</span>';

  const dataSection = c.data ? renderVerifyData(c) : '';

  const checksHtml = (c.checks || []).map(ch =>
    '<div class="verify-check">' +
      '<span class="ck-label">' + ch.label + '</span>' +
      '<span class="ck-value">' +
        (ch.pass ? '<span class="pass">✓</span> ' : '<span class="fail">✗</span> ') +
        ch.value +
      '</span>' +
    '</div>'
  ).join('');

  const statusClass = 'status-' + (c.status_class || 'warning');
  const dotClass = c.status === 'online' ? 'online' : c.status === 'warning' ? 'warning' : 'offline';
  const statusLabel = c.status === 'online' ? '在线' : c.status === 'warning' ? '注意' : '离线';

  return '<div class="verify-card ' + statusClass + '">' +
    '<div class="verify-card-header">' +
      '<span class="icon">' + (c.icon || '📦') + '</span>' +
      '<div class="info">' +
        '<div class="name">' + c.name + '</div>' +
        '<div class="tech">' + (c.tech || '') + '</div>' +
      '</div>' +
      '<span class="status-dot ' + dotClass + '" title="' + statusLabel + '"></span>' +
    '</div>' +
    '<div class="verify-checks">' + checksHtml + '</div>' +
    (dataSection ? '<div style="margin-top:8px;font-size:11px;color:var(--muted)">' + dataSection + '</div>' : '') +
    '<div class="verify-card-actions">' + mgmtBtn + '</div>' +
  '</div>';
}

function renderVerifyData(c) {
  const d = c.data;
  const items = [];
  if (d.console) {
    items.push('🔑 登录: <code>' + d.credentials + '</code>');
  }
  if (d.catalog_tree && d.catalog_tree.length > 0) {
    let treeHtml = '<div style="font-family:monospace;line-height:1.8;margin:4px 0">';
    d.catalog_tree.forEach(ct => {
      treeHtml += '📂 <b>' + ct.name + '</b><br>';
      (ct.schemas || []).forEach(s => {
        treeHtml += '&nbsp;&nbsp;└─ 📁 ' + s.name + '<br>';
        (s.filesets || []).forEach(f => {
          treeHtml += '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└─ 📄 ' + f + '<br>';
        });
      });
    });
    treeHtml += '</div>';
    items.push(treeHtml);
  } else if (d.metalakes) {
    items.push('🏗 Metalakes: ' + d.metalakes.join(', '));
  }
  if (d.sample) {
    const s = d.sample;
    items.push('📝 最新: ' + (s.call_id || '') + ' — 意图:' + (s.caller_intent || s.intent || 'N/A'));
  }
  if (d.image_sample) {
    const imageSample = d.image_sample;
    items.push('🖼 图片样本: ' + (imageSample.doc_id || '') + ' — '
      + (imageSample.is_avatar === true ? '合规头像' : imageSample.is_avatar === false ? '不合规' : (imageSample.analysis_status || '待分析')));
  }
  if (d.sample && d.sample.total_calls !== undefined) {
    items.push('📊 聚合: ' + d.sample.total_calls + ' 通话 / 转网 ' + (d.sample.churn_intent_count || 0) + ' / 高风险 ' + (d.sample.high_risk_count || 0));
  }
  if (d.intent) {
    items.push('🎯 测试意图: ' + d.intent + ' | 风险: ' + (d.risk_level || 'N/A'));
  }
  if (d.query_api) {
    items.push('🔗 <a href="' + API + d.query_api + '" target="_blank">'
      + (d.query_label || '浏览数据') + '</a>');
  }
  return items.length > 0 ? items.join('<br>') : '';
}

// ========== 离线批处理流水线 Demo (4-step: ingest→analyze→embed→query) ==========

const LANCE_URI = '/tmp/offline_demo/calls.lance';

function setOfflineLoading(msg) {
  var results = document.getElementById('offline-results');
  if (!results) return;
  results.style.display = '';
  results.innerHTML =
    '<div style="text-align:center;padding:24px;color:var(--muted)">' +
    '<span class="loading-spin"></span> ' + msg + '</div>';
}

function runOfflineDemo() {
  const btn = document.getElementById('btn-offline-run');
  btn.disabled = true; btn.textContent = '⏳ 运行中...';

  const output = document.getElementById('offline-results');
  output.style.display = '';
  output.innerHTML = '<div id="stream-console" style="background:#1e1e1e;color:#d4d4d4;font-family:Menlo,Monaco,monospace;font-size:11px;border-radius:8px;padding:12px 14px;max-height:400px;overflow-y:auto;line-height:1.7">'
    + '<div style="color:#569cd6">🚀 启动离线批处理流水线...</div></div>'
    + '<div id="stream-summary" style="margin-top:8px;display:none"></div>';

  const consoleEl = document.getElementById('stream-console');
  const es = new EventSource(API + '/api/offline/run-all-stream');

  es.addEventListener('progress', e => {
    const d = JSON.parse(e.data);
    const stepColors = {ingest:'#4fc1ff',transcribe:'#c586c0','analyze-text':'#6a9955',query:'#dcdcaa',gravitino:'#ce9178',pipeline:'#569cd6'};
    const color = stepColors[d.step]||'#d4d4d4';
    let html = '<div>';
    if (d.line) html += '<span style="color:#858585">' + d.line.replace(/\n/g,'<br>  ') + '</span><br>';
    html += '<span style="color:'+color+'">' + d.msg + '</span></div>';
    consoleEl.innerHTML += html;
    consoleEl.scrollTop = consoleEl.scrollHeight;
  });

  es.addEventListener('result', e => {
    const d = JSON.parse(e.data);
    const color = d.status==='done'?'#6a9955':'#ce9178';
    consoleEl.innerHTML += '<div><span style="color:'+color+'">' + d.msg + '</span></div>';
    consoleEl.scrollTop = consoleEl.scrollHeight;
  });

  es.addEventListener('done', e => {
    es.close();
    const d = JSON.parse(e.data);
    consoleEl.innerHTML += '<div style="color:#6a9955;margin-top:4px">' + d.msg + '</div>';

    const summary = document.getElementById('stream-summary');
    summary.style.display = '';
    let html = '<div class="offline-summary-banner">';
    html += '<div class="sb-icon">✅</div>';
    html += '<div class="sb-text"><strong>全流程执行完成！</strong><br>'
      + '历史音频已通过「加载语音 → 转写+情绪 → 智能分析 → 检索」四步处理，'
      + '耗时 ' + d.total_duration_s + 's</div>';
    html += '</div>';
    html += '<div class="offline-meta-row" style="padding:0 4px">';
    html += '<span class="offline-meta-item">📥 入库：<strong>'
      + (d.ingest_rows || '?') + ' 条</strong></span>';
    html += '<span class="offline-meta-item">🔍 分析：<strong>'
      + (d.analyze_count || '?') + ' 条</strong></span>';
    html += '<span class="offline-meta-item">🔊 ANN 命中：<strong>'
      + ((d.ann_top5 || []).length) + ' 条</strong></span>';
    html += '</div>';
    if (d.ann_top5 && d.ann_top5.length) {
      html += '<div style="margin-top:8px;font-size:11px;padding:0 4px">'
        + '<span style="color:var(--muted)">相似通话：</span>';
      d.ann_top5.forEach(function(r, i) {
        html += '<span class="badge badge-low" style="margin-right:4px">'
          + (i + 1) + '. ' + r.doc_id + '</span>';
      });
      html += '</div>';
    }
    summary.innerHTML = html;

    btn.disabled = false; btn.textContent = '▶ 运行全流程';
    loadOverview();
    toast('流水线完成！耗时 ' + d.total_duration_s + 's');
  });

  es.onerror = () => {
    es.close();
    consoleEl.innerHTML += '<div style="color:#ce9178">⚠️ 连接中断</div>';
    btn.disabled = false; btn.textContent = '▶ 运行全流程';
  };
}

function stepByStep(idx) {
  // Step 4 (数据检索) 展开检索面板，用户可以输入自定义条件
  if (idx === 3) {
    showSearchPanel();
    return;
  }

  const steps = [
    {url: '/api/offline/ingest', body: '{}', label: '加载语音 · 录音入库中'},
    {url: '/api/offline/transcribe', body: '{}', label: '语音转文字 · 提炼情绪标签'},
    {stream: '/api/offline/analyze-text-stream', label: '智能分析 · PII脱敏+LLM+嵌入'},
  ];

  // Step 3 (智能分析) 使用 SSE 流式进度
  if (idx === 2 && steps[idx].stream) {
    stepByStepStream(idx, steps[idx]);
    return;
  }

  const s = steps[idx];
  setOfflineLoading(s.label + ' 执行中...');

  let fetchUrl = API + s.url;
  if (s.qs) fetchUrl += s.qs;
  const opts = {method: 'POST', headers: {'Content-Type': 'application/json'}};
  if (s.body) opts.body = s.body;

  fetch(fetchUrl, opts)
    .then(r => r.json())
    .then(d => {
      renderSingleStep(d, idx);
      loadOverview();
    })
    .catch(e => {
      document.getElementById('offline-result').innerHTML =
        '<div style="padding:12px;border-radius:8px;background:#FCEBEB;color:var(--red);font-size:13px">' + s.label + ' 失败: ' + e.message + '</div>';
    });
}

// ──────────── 步骤4 检索面板 ────────────
function showSearchPanel() {
  var panel = document.getElementById('search-panel');
  if (panel) panel.style.display = '';
  // 默认展示语气差 (bad_tone = true) 的示例结果
  setTimeout(function() { runSearch(); }, 300);
}

function onSearchTypeChange() {
  var type = document.getElementById('search-type').value;
  var condInput = document.getElementById('search-condition');
  var docInput = document.getElementById('search-doc-id');
  if (type === 'ann') {
    condInput.style.display = 'none';
    docInput.style.display = '';
  } else {
    condInput.style.display = '';
    docInput.style.display = 'none';
  }
}

function setSearchPreset(cond) {
  document.getElementById('search-condition').value = cond;
  document.getElementById('search-type').value = 'scalar';
  onSearchTypeChange();
  runSearch();
}

function runSearch() {
  var type = document.getElementById('search-type').value;
  var topk = parseInt(document.getElementById('search-topk').value) || 5;
  var body = {query_type: type, top_k: topk};

  if (type === 'ann') {
    body.query_doc_id = document.getElementById('search-doc-id').value.trim()
      || 'call_006_churn_angry.txt';
  } else {
    var cond = document.getElementById('search-condition').value.trim();
    if (cond) {
      body.where = cond;
    }
  }

  setOfflineLoading('检索中...');
  fetch(API + '/api/offline/query', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.error) {
        document.getElementById('offline-result').innerHTML =
          '<div style="padding:12px;border-radius:8px;background:#FFF5F5;color:var(--red);font-size:13px">检索失败: ' + d.error + '</div>';
        return;
      }
      renderQueryResults(d);
    })
    .catch(function(e) {
      document.getElementById('offline-result').innerHTML =
        '<div style="padding:12px;border-radius:8px;background:#FCEBEB;color:var(--red);font-size:13px">检索失败: ' + e.message + '</div>';
    });
}

function stepByStepStream(idx, stepDef) {
  const output = document.getElementById('offline-result');
  output.style.display = '';
  output.innerHTML = '<div id="stream-console" style="background:#1e1e1e;color:#d4d4d4;font-family:Menlo,Monaco,monospace;font-size:11px;border-radius:8px;padding:12px 14px;max-height:400px;overflow-y:auto;line-height:1.7">'
    + '<div style="color:#569cd6">📝 启动智能分析（PII脱敏 + LLM + 向量嵌入）...</div></div>'
    + '<div id="stream-summary" style="margin-top:8px;display:none"></div>';

  const consoleEl = document.getElementById('stream-console');
  const es = new EventSource(API + stepDef.stream);

  es.addEventListener('start', function(e) {
    const d = JSON.parse(e.data);
    consoleEl.innerHTML += '<div style="color:#6a9955">  共 ' + d.total
      + ' 条通话待分析，逐条处理中...</div>';
    consoleEl.scrollTop = consoleEl.scrollHeight;
  });

  es.addEventListener('progress', function(e) {
    const d = JSON.parse(e.data);
    const pct = Math.round(d.current / d.total * 100);
    consoleEl.innerHTML += '<div style="color:#d4d4d4">  ['
      + d.current + '/' + d.total + '] ' + pct + '% LLM分析: '
      + d.doc_id + '</div>';
    consoleEl.scrollTop = consoleEl.scrollHeight;
  });

  es.addEventListener('done', function(e) {
    es.close();
    const d = JSON.parse(e.data);
    consoleEl.innerHTML += '<div style="color:#6a9955;margin-top:4px">✅ 智能分析完成！'
      + d.processed + ' 条，耗时 ' + d.duration_s + 's</div>';

    // 渲染分析结果卡片
    const summary = document.getElementById('stream-summary');
    summary.style.display = '';
    summary.innerHTML = '<div class="offline-result-card">'
      + '<div class="step-header" style="border-left-color:var(--green)">'
      + '<div class="step-dot" style="background:var(--green)">3</div>'
      + '<div class="step-info">'
      + '<div class="step-name">📝 智能分析</div>'
      + '<div class="step-purpose">PII脱敏、LLM意图分析、向量嵌入</div>'
      + '</div>'
      + '<div class="step-dur">⏱ ' + d.duration_s + 's</div>'
      + '</div>'
      + '<div class="step-body">'
      + renderAnalyzeCards(d.results)
      + '</div></div>';
    loadOverview();
  });

  es.onerror = function() {
    es.close();
    consoleEl.innerHTML += '<div style="color:#ce9178">⚠️ 连接中断</div>';
  };
}

function renderOffsetResult(d) {
  // Legacy support
  if (d.steps) { renderPipelineResult(d); return; }
  if (typeof d.type === 'string') { renderQueryResults(d); return; }
  renderSingleStep(d, 0);
}

// Shared helper: render risk distribution bars
function renderRiskBars(riskDist) {
  if (!riskDist) return '';
  const rd = {high: riskDist.high || 0, medium: riskDist.medium || 0, low: riskDist.low || 0};
  const total = rd.high + rd.medium + rd.low || 1;
  const pct = function(v) { return (v / total * 100).toFixed(0); };
  return '<div class="offline-risk-overview">'
    + '<div class="offline-risk-bar-item">'
    + '<div class="rbi-count" style="color:var(--red)">' + rd.high + '</div>'
    + '<div class="rbi-label">🔴 高风险</div></div>'
    + '<div class="offline-risk-bar-item">'
    + '<div class="rbi-count" style="color:var(--amber)">' + rd.medium + '</div>'
    + '<div class="rbi-label">🟡 中风险</div></div>'
    + '<div class="offline-risk-bar-item">'
    + '<div class="rbi-count" style="color:var(--green)">' + rd.low + '</div>'
    + '<div class="rbi-label">🟢 低风险</div></div>'
    + '</div>'
    + '<div class="offline-risk-bar-visual">'
    + '<div style="width:' + pct(rd.high) + '%;background:var(--red);height:8px"></div>'
    + '<div style="width:' + pct(rd.medium) + '%;background:var(--amber);height:8px"></div>'
    + '<div style="width:' + pct(rd.low) + '%;background:var(--green);height:8px"></div>'
    + '</div>';
}

// Shared helper: render analysis cards
function renderAnalyzeCards(results) {
  if (!results || results.length === 0) {
    return '<div style="font-size:12px;color:var(--muted)">暂无分析结果</div>';
  }
  const reasons = {};
  results.forEach(function(r) {
    const key = r.primary_reason || '其他';
    reasons[key] = (reasons[key] || 0) + 1;
  });
  let html = '<div class="result-label">📊 分析概览</div>';
  html += '<div style="font-size:13px;margin-bottom:8px">'
    + '<span style="color:var(--green);font-weight:600">✅ 已完成 '
    + results.length + ' 条通话的 AI 分析</span></div>';
  // Tag cloud of reasons
  if (Object.keys(reasons).length > 0) {
    html += '<div style="margin-bottom:12px;font-size:11px;color:var(--muted)">'
      + '🔑 主要归因：';
    Object.entries(reasons).forEach(function(e, i) {
      html += '<span class="ai-tag" style="margin-left:4px">' + e[0]
        + ' ×' + e[1] + '</span>';
    });
    html += '</div>';
  }
  // Individual cards
  html += '<div class="offline-analysis-cards">';
  results.slice(0, 5).forEach(function(r) {
    const isRisk = r.downgrade_related || r.bad_tone;
    const cls = isRisk ? 'ai-high' : 'ai-low';
    html += '<div class="offline-analysis-item ' + cls + '">';
    html += '<span class="ai-call">📞 ' + r.doc_id + '</span>';
    html += '<span class="badge ' + (isRisk ? 'badge-high' : 'badge-low') + '">'
      + (isRisk ? '⚠️ 有风险' : '✅ 正常') + '</span>';
    if (r.bad_tone) {
      html += '<span class="badge badge-neg">语气差</span>';
    }
    html += '<div class="ai-content">';
    html += '<div style="font-size:11px">原因：' + (r.primary_reason || '其他')
      + ' ｜ 情绪：' + (r.text_emotion || '未知')
      + ' ｜ 时长：' + (r.duration_s || '?') + 's</div>';
    if (r.transcript) {
      html += '<div class="ai-transcript">'
        + (r.transcript || '').substring(0, 200) + '…</div>';
    }
    html += '</div></div>';
  });
  if (results.length > 5) {
    html += '<div style="font-size:10px;color:var(--muted);text-align:center;padding:4px">'
      + '… 还有 ' + (results.length - 5) + ' 条结果</div>';
  }
  html += '</div>';
  html += '<div style="font-size:10px;color:var(--muted);margin-top:10px">'
    + '💡 AI 已自动为每条通话打上风险标签和情绪标签，方便后续检索</div>';
  return html;
}

// Shared helper: cluster placeholder for embed step
function renderClusterCards(d) {
  const dim = d.dim || 128;
  let html = '<div style="display:flex;align-items:flex-start;gap:16px;flex-wrap:wrap">';
  html += '<div style="flex:1;min-width:160px">';
  html += '<div style="font-size:13px;margin-bottom:6px;color:var(--green);font-weight:600">'
    + '📐 ' + dim + '维声学特征向量</div>';
  html += '<div style="font-size:11px;color:var(--muted);line-height:1.6">'
    + '提取了每条录音的<strong>语音特征</strong>（响度、音调、语速变化等），'
    + '将通话"语气"转化为数学向量。<br><br>'
    + '语气相似的通话在向量空间中<strong>距离更近</strong>，'
    + '可以一键找出"语气最接近XXX的那些通话"。</div>';
  html += '</div>';
  html += '<div class="offline-cluster-grid" style="flex:1.5">';
  // Simulated clusters
  var clusters = [
    {icon: '😡', label: '愤怒/投诉类', count: '约 2-3 条', members: 'call_003, call_006…', bg: '#FFF5F5', border: '#FCC'},
    {icon: '😐', label: '犹豫/比价类', count: '约 2-3 条', members: 'call_001, call_004…', bg: '#FFFCF5', border: '#FED'},
    {icon: '🙂', label: '正常/咨询类', count: '约 2-3 条', members: 'call_002, call_005…', bg: '#F5FFFA', border: '#CFC'}];
  clusters.forEach(function(c) {
    html += '<div class="offline-cluster-group" style="background:' + c.bg
      + ';border-color:' + c.border + '">';
    html += '<div class="cg-icon">' + c.icon + '</div>';
    html += '<div class="cg-label">' + c.label + '</div>';
    html += '<div class="cg-count">' + c.count + '</div>';
    html += '<div class="cg-members">' + c.members + '</div>';
    html += '</div>';
  });
  html += '</div></div>';
  html += '<div class="offline-meta-row" style="margin-top:10px">';
  html += '<span class="offline-meta-item">列名：<strong>audio_emb</strong></span>';
  html += '<span class="offline-meta-item">维度：<strong>' + dim + 'd float32</strong></span>';
  html += '<span class="offline-meta-item">状态：<strong>已追加到数据表</strong></span>';
  html += '</div>';
  return html;
}

// Shared helper: render search results
function renderSearchContent(d) {
  var isAnn = d.type === 'ann';
  var matched = d.matched || 0;
  var results = d.results || [];
  var html = '';

  // 空结果兜底
  if (results.length === 0) {
    html += '<div style="text-align:center;padding:20px;color:var(--muted)">'
      + '<div style="font-size:40px;margin-bottom:8px">🔍</div>'
      + '<div>没有匹配到结果，请尝试调整检索条件</div></div>';
    return html;
  }

  html += '<div class="result-label">📊 检索结果</div>';
  html += '<div style="font-size:13px;margin-bottom:10px">'
    + '<span style="color:var(--green);font-weight:600">✅ 匹配到 '
    + matched + ' 条结果</span>';
  if (isAnn) {
    html += '<span style="font-size:11px;color:var(--muted);margin-left:8px">'
      + '基于声学特征相似度排序</span>';
  } else {
    html += '<span style="font-size:11px;color:var(--muted);margin-left:8px">'
      + '耗时 ' + (d.duration_s != null ? d.duration_s + 's' : '') + '</span>';
  }
  html += '</div>';

  // 统计摘要
  var dangerCount = 0;
  var negCount = 0;
  results.forEach(function(r) {
    if (r.bad_tone) dangerCount++;
    if (r.text_emotion === 'negative') negCount++;
  });
  html += '<div style="font-size:11px;color:var(--muted);margin-bottom:10px;line-height:1.8">'
    + '📈 统计：<span style="color:var(--red);font-weight:600">语气差 '
    + dangerCount + '</span> 条 ｜ 负面情绪 ' + negCount + ' 条'
    + ' ｜ 降套餐 ' + results.filter(function(r){return r.downgrade_related;}).length + ' 条</div>';

  html += '<div class="offline-search-section">';
  html += '<div class="ss-title">' + (isAnn ? '🔊 ANN 向量检索'
    : '🔍 标量条件过滤') + ' — Top ' + results.length + '</div>';

  results.forEach(function(r, i) {
    var distancePct = '';
    if (r._distance != null) {
      var pct = Math.max(0, (1 - r._distance) * 100).toFixed(0);
      distancePct = '<span class="si-score">相似度 ' + pct
        + '% (d=' + r._distance.toFixed(3) + ')</span>';
    }
    var toneBadge = '';
    if (r.bad_tone) {
      toneBadge = '<span class="badge badge-neg">语气差</span>';
    }
    var downgradeBadge = '';
    if (r.downgrade_related) {
      downgradeBadge = '<span class="badge badge-high">降套餐</span>';
    }
    html += '<div style="padding:10px;margin-bottom:6px;border-radius:6px;'
      + 'background:' + (r.bad_tone ? 'var(--bg3)' : 'var(--bg1)') + ';'
      + 'border-left:3px solid ' + (r.bad_tone ? 'var(--red)' : 'var(--border)') + '">';
    html += '<div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:4px">';
    html += '<span class="si-call">' + (i + 1) + '. 📞 ' + r.doc_id + '</span>';
    html += toneBadge + downgradeBadge;
    if (r.primary_reason) {
      html += '<span class="badge badge-med">' + r.primary_reason + '</span>';
    }
    if (r.text_emotion) {
      html += '<span class="badge badge-neg">' + r.text_emotion + '</span>';
    }
    html += distancePct;
    html += '</div>';
    if (r.summary) {
      html += '<div style="font-size:11px;color:var(--muted);margin-top:4px;line-height:1.5">'
        + '📝 ' + r.summary + '</div>';
    }
    if (r.transcript) {
      var txt = String(r.transcript || '');
      if (txt.length > 100) txt = txt.substring(0, 100) + '…';
      html += '<div style="font-size:10px;color:var(--muted);margin-top:4px;line-height:1.5">'
        + '💬 ' + txt + '</div>';
    }
    html += '</div>';
  });
  html += '</div>';

  html += '<div style="font-size:10px;color:var(--muted);margin-top:10px">'
    + '💡 ' + (isAnn
    ? 'ANN 检索可在海量数据中毫秒级找出语气最相似的通话'
    : '标量查询支持任意字段组合过滤（bad_tone, downgrade_related, text_emotion, primary_reason 等），秒级返回结果')
    + '<br>试试切换「标量/ANN」或选择下方「快捷条件」</div>';
  return html;
}

function renderPipelineResult(d) {
  const steps = d.steps || [];
  const purposes = [
    '把分散在各处的通话录音统一入库管理',
    '语音转文字并提炼声学情绪标签',
    'PII脱敏、LLM意图分析、向量嵌入',
    '标量+向量检索，秒级找出风险通话'
  ];
  const colors = ['var(--blue)', 'var(--accent)', 'var(--green)', 'var(--amber)'];
  const stepNames = ['📥 加载语音', '🎙️ 转写+情绪', '📝 智能分析', '🎯 一键检索'];
  let html = '';

  // Summary banner
  html += '<div class="offline-summary-banner">';
  html += '<div class="sb-icon">✅</div>';
  html += '<div class="sb-text"><strong>流水线执行完成</strong><br>'
    + '历史音频已成功完成「加载语音 → 转写+情绪 → 智能分析 → 检索」全链路处理，'
    + '耗时 ' + d.total_duration_s + 's</div>';
  html += '</div>';

  // Step cards
  html += '<div style="display:flex;flex-direction:column;gap:10px">';
  steps.forEach(function(s, i) {
    if (!s || !s.step) return;
    if (s.step === 'gravitino' || s.step === 'gravitino_register') return;
    // Map step name to display index
    var stepIdxMap = {ingest:0, transcribe_and_tag:1, analyze_text:2, query:3};
    var stepIdx = stepIdxMap[s.step] || i;
    var color = colors[stepIdx] || 'var(--border)';
    html += '<div class="offline-result-card">';
    // Header
    html += '<div class="step-header" style="border-left-color:' + color + '">';
    html += '<div class="step-dot" style="background:' + color + '">'
      + (stepIdx + 1) + '</div>';
    html += '<div class="step-info">';
    html += '<div class="step-name">' + (stepNames[stepIdx] || s.step) + '</div>';
    html += '<div class="step-purpose">'
      + (purposes[stepIdx] || '') + '</div>';
    html += '</div>';
    if (s.duration_s != null) {
      html += '<div class="step-dur">⏱ ' + s.duration_s + 's</div>';
    }
    html += '</div>';
    // Body
    html += '<div class="step-body">';
    if (s.step === 'ingest') {
      html += renderIngestBody(s);
    } else if (s.step === 'transcribe_and_tag') {
      html += renderTranscribeCards(s.results);
    } else if (s.step === 'analyze_text') {
      html += renderAnalyzeCards(s.results);
    } else if (s.step === 'query') {
      var qt = s.ann_top5 ? 'ann' : 'scalar';
      var qr = {type: qt, matched: s.matched || s.scalar_matched,
        results: s.scalar_top5 || s.ann_top5 || s.results};
      html += renderSearchContent(qr);
    }
    html += '</div></div>';
  });
  html += '</div>';
  document.getElementById('offline-result').innerHTML = html;
}

function renderSingleStep(d, idx) {
  const purposes = [
    '把分散在各处的通话录音统一入库管理',
    '语音转文字并提炼声学情绪标签',
    'PII脱敏、LLM意图分析、向量嵌入'
  ];
  const labels = ['📥 加载语音', '🎙️ 转写+情绪', '📝 智能分析'];
  const colors = ['var(--blue)', 'var(--accent)', 'var(--green)'];
  let html = '<div class="offline-result-card">';
  html += '<div class="step-header" style="border-left-color:' + colors[idx] + '">';
  html += '<div class="step-dot" style="background:' + colors[idx] + '">'
    + (idx + 1) + '</div>';
  html += '<div class="step-info">';
  html += '<div class="step-name">' + labels[idx] + '</div>';
  html += '<div class="step-purpose">' + purposes[idx] + '</div>';
  html += '</div>';
  if (d.duration_s) {
    html += '<div class="step-dur">⏱ ' + d.duration_s + 's</div>';
  }
  html += '</div>';
  html += '<div class="step-body">';
  if (idx === 0) {
    html += renderIngestBody(d);
  } else if (idx === 1) {
    html += renderTranscribeCards(d.results);
  } else if (idx === 2) {
    html += renderAnalyzeCards(d.results);
  }
  html += '</div></div>';
  document.getElementById('offline-result').innerHTML = html;
}

function renderTranscribeCards(results) {
  if (!results || results.length === 0) {
    return '<div style="font-size:12px;color:var(--muted)">暂无转写结果</div>';
  }
  var emotions = {};
  results.forEach(function(r) {
    var key = r.acoustic_emotion || 'NEUTRAL';
    emotions[key] = (emotions[key] || 0) + 1;
  });
  var emojiMap = {ANGRY:'😡', SAD:'😢', NEUTRAL:'😐', HAPPY:'😊'};
  var labelMap = {ANGRY:'愤怒', SAD:'悲伤', NEUTRAL:'中性', HAPPY:'开心'};
  var colorMap = {ANGRY:'var(--red)', SAD:'var(--amber)', NEUTRAL:'var(--muted)', HAPPY:'var(--green)'};
  var html = '<div class="result-label">📊 转写+情绪概览</div>';
  html += '<div style="font-size:13px;margin-bottom:8px">'
    + '<span style="color:var(--green);font-weight:600">✅ 已完成 '
    + results.length + ' 条通话的ASR转写和情绪标签</span></div>';

  // ASR 转写验证概览
  var accuracies = [];
  results.forEach(function(r) {
    if (r.asr_match && r.asr_match.char_accuracy != null) {
      accuracies.push(r.asr_match.char_accuracy);
    }
  });
  if (accuracies.length > 0) {
    var avgAcc = (accuracies.reduce(function(a,b){return a+b;}, 0) / accuracies.length).toFixed(1);
    var minAcc = Math.min.apply(null, accuracies).toFixed(1);
    var maxAcc = Math.max.apply(null, accuracies).toFixed(1);
    var accColor = avgAcc >= 95 ? '#0F6E56' : (avgAcc >= 85 ? '#854F0B' : '#993C1D');
    html += '<div style="margin-bottom:12px;padding:8px 12px;background:#F0F9F0;'
      + 'border:1px solid #A8D8A8;border-radius:6px;font-size:12px;line-height:1.8">'
      + '<span style="font-weight:600;color:' + accColor + '">🔍 ASR 转写验证</span><br>'
      + '<span style="color:var(--muted)">字符匹配率：平均 </span>'
      + '<strong style="color:' + accColor + '">' + avgAcc + '%</strong>'
      + '<span style="color:var(--muted)"> （' + results.length + ' 条样本，'
      + '最高 ' + maxAcc + '% / 最低 ' + minAcc + '%）</span><br>'
      + '<span style="font-size:10px;color:var(--muted);line-height:2">'
      + '图例：<span style="color:var(--red);font-weight:600;'
      + 'text-decoration:underline;text-underline-offset:2px;'
      + 'text-decoration-color:var(--red);text-decoration-style:wavy">'
      + '识别错误</span>（波浪下划线） | '
      + '<span style="color:var(--amber);font-style:italic;font-weight:500;'
      + 'background:#FFF8E1;padding:0 2px;border-radius:2px">'
      + 'ASR额外添加</span>（橙色斜体） | '
      + '<span style="color:var(--red);text-decoration:line-through;'
      + 'opacity:0.7">原文漏识别</span>（红色删除线）'
      + '</span></div>';
  }

  html += '<div style="margin-bottom:12px;font-size:11px;color:var(--muted)">'
    + '🎭 声学情绪分布：';
  Object.entries(emotions).forEach(function(e) {
    var emoji = emojiMap[e[0]] || '❓';
    var label = labelMap[e[0]] || e[0];
    html += '<span class="ai-tag" style="margin-left:4px">'
      + emoji + ' ' + label + ' ×' + e[1] + '</span>';
  });
  html += '</div>';
  html += '<div class="offline-analysis-cards">';
  results.slice(0, 5).forEach(function(r) {
    var emoji = emojiMap[r.acoustic_emotion] || '❓';
    var label = labelMap[r.acoustic_emotion] || r.acoustic_emotion;
    var matchInfo = r.asr_match || {};
    var accuracy = matchInfo.char_accuracy != null ? matchInfo.char_accuracy.toFixed(1) : '--';
    var cf = matchInfo.asr_confidence != null ? matchInfo.asr_confidence.toFixed(2) : '--';
    var accClr = accuracy >= 95 ? 'var(--green)' : (accuracy >= 85 ? 'var(--amber)' : 'var(--red)');
    var subCnt = matchInfo.sub_count || 0;
    var insCnt = matchInfo.ins_count || 0;
    var delCnt = matchInfo.del_count || 0;
    html += '<div class="offline-analysis-item" style="border-left:3px solid '
      + (colorMap[r.acoustic_emotion] || 'var(--border)') + '">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center">';
    html += '<span class="ai-call">📞 ' + r.doc_id + '</span>';
    html += '<span class="badge" style="background:#FFF5F5;color:var(--red)">'
      + emoji + ' ' + label + '</span>';
    html += '</div>';
    html += '<div class="ai-content">';
    html += '<div style="font-size:11px">⏱ 时长：' + (r.duration_s || '?')
      + 's ｜ 情绪：' + (r.acoustic_emotion || 'NEUTRAL')
      + ' ｜ <span style="color:' + accClr + ';font-weight:500">ASR匹配率：'
      + accuracy + '%</span>'
      + ' ｜ <span style="color:' + accClr + '">置信度：' + cf + '</span>';
    if (subCnt + insCnt + delCnt > 0) {
      html += ' ｜ <span style="color:var(--red);font-size:10px">✗替换' + subCnt
        + '</span> <span style="color:var(--amber);font-size:10px">+插入' + insCnt
        + '</span> <span style="color:var(--red);font-size:10px">−删除' + delCnt + '</span>';
    }
    html += '</div>';

    // ASR 差异高亮：renderDiffHtml() 将 diff_segments 转为带颜色标注的 HTML
    var segs = matchInfo.diff_segments;
    if (r.transcript && segs && segs.length > 0) {
      var escHtml = function(s) {
        return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
          .replace(/>/g, '&gt;').replace(/"/g, '&quot;')
          .replace(/\n/g, '<br>');
      };
      var renderDiffHtml = function(segmentList, maxChars) {
        var out = '';
        var cnt = 0;
        for (var si = 0; si < segmentList.length; si++) {
          var seg = segmentList[si];
          var orig = seg.original || '';
          var trans = seg.transcribed || '';
          if (maxChars && cnt >= maxChars) { out += '…'; break; }
          switch (seg.type) {
            case 'match':
              out += escHtml(orig);
              cnt += orig.length;
              break;
            case 'sub':
              out += '<span style="color:var(--red);font-weight:600;'
                + 'text-decoration:underline;text-underline-offset:3px;'
                + 'text-decoration-color:var(--red);text-decoration-style:wavy"'
                + ' title="原文: ' + escHtml(orig) + '">'
                + escHtml(trans) + '</span>';
              cnt += trans.length;
              break;
            case 'ins':
              out += '<span style="color:var(--amber);font-style:italic;'
                + 'font-weight:500;background:#FFF8E1;padding:0 1px;'
                + 'border-radius:2px" title="ASR额外添加">'
                + escHtml(trans) + '</span>';
              cnt += trans.length;
              break;
            case 'del':
              out += '<span style="color:var(--red);text-decoration:line-through;'
                + 'opacity:0.7" title="ASR漏识别: ' + escHtml(orig) + '">'
                + escHtml(orig) + '</span>';
              cnt += orig.length;
              break;
            default:
              out += escHtml(trans || orig);
              cnt += (trans || orig).length;
          }
        }
        return out;
      };
      var docIdSafe = (r.doc_id || '').replace(/[^a-zA-Z0-9_-]/g, '_');
      var uniqId = 'tx_' + docIdSafe;
      var diffShort = renderDiffHtml(segs, 200);
      html += '<div class="ai-transcript" id="' + uniqId + '_short">'
        + diffShort + '</div>';
      var transcriptLen = (r.transcript || '').length;
      if (transcriptLen > 200) {
        var diffFull = renderDiffHtml(segs);
        html += '<div class="ai-transcript" id="' + uniqId + '_full"'
          + ' style="display:none">' + diffFull + '</div>';
        html += '<span style="font-size:10px;color:var(--accent);cursor:pointer;'
          + 'margin-top:2px;display:inline-block"'
          + ' onclick="var s=document.getElementById(\'' + uniqId
          + '_short\');var f=document.getElementById(\'' + uniqId
          + '_full\');var t=this;'
          + 'if(s.style.display===\'none\'){s.style.display=\'\';'
          + 'f.style.display=\'none\';t.textContent=\'展开全文 ▼\'}'
          + 'else{s.style.display=\'none\';f.style.display=\'\';'
          + 't.textContent=\'收起 ▲\'}">展开全文 ▼</span>';
      }
    } else if (r.transcript) {
      // 无 diff 数据时，回退纯文本展示
      var fullText = (r.transcript || '');
      var preview = fullText.length > 200
        ? fullText.substring(0, 200) + '…'
        : fullText;
      var docIdSafe = (r.doc_id || '').replace(/[^a-zA-Z0-9_-]/g, '_');
      var uniqId = 'tx_' + docIdSafe;
      html += '<div class="ai-transcript" id="' + uniqId + '_short">'
        + preview + '</div>';
      if (fullText.length > 200) {
        html += '<div class="ai-transcript" id="' + uniqId
          + '_full" style="display:none">'
          + fullText.replace(/\n/g, '<br>') + '</div>';
        html += '<span style="font-size:10px;color:var(--accent);cursor:pointer;'
          + 'margin-top:2px;display:inline-block"'
          + ' onclick="var s=document.getElementById(\'' + uniqId
          + '_short\');var f=document.getElementById(\'' + uniqId
          + '_full\');var t=this;'
          + 'if(s.style.display===\'none\'){s.style.display=\'\';'
          + 'f.style.display=\'none\';t.textContent=\'展开全文 ▼\'}'
          + 'else{s.style.display=\'none\';f.style.display=\'\';'
          + 't.textContent=\'收起 ▲\'}">展开全文 ▼</span>';
      }
    }
    // 语音回放
    var audioUrl = r.audio_url || ('/api/offline/audio/' + r.doc_id);
    html += '<div style="margin-top:6px"><audio controls preload="none" src="'
      + audioUrl + '" style="height:28px;width:100%;max-width:320px"></audio></div>';
    html += '</div></div>';
  });
  if (results.length > 5) {
    html += '<div style="font-size:10px;color:var(--muted);text-align:center;padding:4px">'
      + '… 还有 ' + (results.length - 5) + ' 条结果</div>';
  }
  html += '</div>';
  return html;
}

function renderIngestBody(d) {
  let html = '<div class="result-label">📊 入库结果</div>';
  html += '<div style="font-size:13px;margin-bottom:10px">'
    + '<span style="color:var(--green);font-weight:600">✅ 已成功入库 '
    + (d.rows || 0) + ' 条通话录音</span>'
    + '<span style="font-size:11px;color:var(--muted);margin-left:8px">'
    + '存储位置：安全数据湖</span></div>';
  html += '<div class="offline-audio-list">';
  const audioUrls = d.audio_urls || {};
  if (d.doc_ids) {
    d.doc_ids.forEach(function(docId) {
      const au = audioUrls[docId] || ('/api/offline/audio/' + docId);
      html += '<div class="offline-audio-item">';
      html += '<span class="ai-name">📞 ' + docId + '</span>';
      html += '<audio controls preload="none" src="' + au
        + '" style="height:30px;flex:1;max-width:280px"></audio>';
      html += '</div>';
    });
  }
  html += '</div>';
  html += '<div style="font-size:10px;color:var(--muted);margin-top:8px">'
    + '💡 点击播放按钮试听已入库的通话录音</div>';
  return html;
}

function renderQueryResults(d) {
  var isAnn = d.type === 'ann';
  var color = 'var(--amber)';
  var label = isAnn ? '🔊 ANN 向量检索' : '🔍 标量条件过滤';
  var purpose = isAnn
    ? '按声学相似度检索，找出与指定通话语气最接近的其他通话'
    : '按条件过滤，秒级找出符合条件的所有通话';
  var conditionInfo = '';
  if (isAnn) {
    conditionInfo = '参考通话: ' + (d.query_doc_id || '?');
  } else {
    conditionInfo = '条件: <code style="background:var(--bg1);padding:1px 6px;border-radius:3px;font-size:11px">'
      + (d.effective_where || d.where || '全部') + '</code>';
  }
  var html = '<div class="offline-result-card">';
  html += '<div class="step-header" style="border-left-color:' + color + '">';
  html += '<div class="step-dot" style="background:' + color + '">4</div>';
  html += '<div class="step-info">';
  html += '<div class="step-name">🎯 一键检索</div>';
  html += '<div class="step-purpose">' + purpose + '<br>'
    + '<span style="font-size:10px;color:var(--muted)">' + conditionInfo + '</span></div>';
  html += '</div>';
  if (d.duration_s != null) {
    html += '<div class="step-dur">⏱ ' + d.duration_s + 's</div>';
  }
  html += '</div>';
  html += '<div class="step-body">';
  html += renderSearchContent(d);
  html += '</div></div>';
  document.getElementById('offline-result').innerHTML = html;
}

// ========== 图片处理流水线 ==========

function imageRequest(path, options) {
  return fetch(API + path, options).then(function(response) {
    return response.json().then(function(data) {
      if (!response.ok) throw new Error(data.detail || data.message || ('HTTP ' + response.status));
      return data;
    });
  });
}

function loadImageStatus() {
  imageRequest('/api/image/status').then(function(data) {
    var models = data.models || {};
    var text = 'ChineseCLIP: ' + (models.chinese_clip_loaded ? '已加载' : '首次运行时加载');
    if (models.vlm_configured) {
      text += ' · VLM: ' + escapeHtml(models.vlm_model || '已配置');
    } else {
      text += ' · VLM 未配置 (' + escapeHtml((models.vlm_missing_config || []).join(', ')) + ')';
    }
    document.getElementById('image-model-status').innerHTML = text;
  }).catch(function(error) {
    document.getElementById('image-model-status').textContent = '模型状态读取失败：' + error.message;
  });
}

function imageConsoleLine(message, color) {
  var consoleEl = document.getElementById('image-console');
  consoleEl.style.display = '';
  consoleEl.innerHTML += '<div style="color:' + (color || '#d4d4d4') + '">' + escapeHtml(message) + '</div>';
  consoleEl.scrollTop = consoleEl.scrollHeight;
}

function runImagePipeline() {
  var backend = document.getElementById('image-analysis-backend').value;
  var button = document.getElementById('btn-image-run');
  button.disabled = true;
  button.textContent = '运行中...';
  document.getElementById('image-console').innerHTML = '';
  document.getElementById('image-console').style.display = '';
  document.getElementById('image-summary').innerHTML = '';
  imageConsoleLine('启动图片流水线，合规后端：' + backend, '#569cd6');

  var es = new EventSource(API + '/api/image/run-all-stream?analysis_backend=' + encodeURIComponent(backend));
  var finished = false;
  es.addEventListener('stage', function(event) {
    var data = JSON.parse(event.data);
    imageConsoleLine('[' + data.index + '/4] ' + data.label, '#dcdcaa');
  });
  es.addEventListener('progress', function(event) {
    var data = JSON.parse(event.data);
    imageConsoleLine('  [' + data.current + '/' + data.total + '] ' + data.doc_id + ' · ' + data.msg, '#858585');
  });
  es.addEventListener('result', function(event) {
    var data = JSON.parse(event.data);
    if (data.step === 'analyze') renderImageGallery(data.results || []);
    if (data.step === 'query') renderImageQueryResults(data);
    imageConsoleLine('  完成 ' + (data.step || data.type) + (data.duration_s != null ? ' (' + data.duration_s + 's)' : ''), '#6a9955');
  });
  es.addEventListener('done', function(event) {
    finished = true;
    var data = JSON.parse(event.data);
    es.close();
    imageConsoleLine('全部完成，总耗时 ' + data.total_duration_s + 's', '#6a9955');
    button.disabled = false;
    button.textContent = '重新运行图片流水线';
    loadImageStatus();
    loadImageLanceData();
    toast('图片流水线执行完成');
  });
  es.addEventListener('error', function(event) {
    if (event.data) {
      finished = true;
      var data = JSON.parse(event.data);
      imageConsoleLine('失败：' + data.message, '#ce9178');
      es.close();
      button.disabled = false;
      button.textContent = '启动图片流水线';
      toast('图片流水线失败', 'error');
    }
  });
  es.onerror = function() {
    if (finished) return;
    es.close();
    imageConsoleLine('SSE 连接中断', '#ce9178');
    button.disabled = false;
    button.textContent = '启动图片流水线';
  };
}

function runImageStep(step) {
  var options = {method: 'POST', headers: {'Content-Type': 'application/json'}};
  if (step === 'analyze') {
    options.body = JSON.stringify({analysis_backend: document.getElementById('image-analysis-backend').value});
  }
  imageConsoleLine('执行 ' + step + '...', '#569cd6');
  imageRequest('/api/image/' + step, options).then(function(data) {
    imageConsoleLine(step + ' 完成 (' + (data.duration_s || 0) + 's)', '#6a9955');
    if (data.results) renderImageGallery(data.results);
    loadImageStatus();
    loadImageLanceData();
  }).catch(function(error) {
    imageConsoleLine(step + ' 失败：' + error.message, '#ce9178');
    toast(step + ' 失败', 'error');
  });
}

function onImageQueryTypeChange() {
  var textMode = document.getElementById('image-query-type').value === 'text';
  document.getElementById('image-query-text').style.display = textMode ? '' : 'none';
  document.getElementById('image-query-where').style.display = textMode ? 'none' : '';
}

function setImageTextQuery(text) {
  document.getElementById('image-query-type').value = 'text';
  document.getElementById('image-query-text').value = text;
  onImageQueryTypeChange();
  runImageQuery();
}

function setImageScalarQuery(where) {
  document.getElementById('image-query-type').value = 'scalar';
  document.getElementById('image-query-where').value = where;
  onImageQueryTypeChange();
  runImageQuery();
}

function runImageQuery() {
  var type = document.getElementById('image-query-type').value;
  var body = {
    query_type: type,
    top_k: parseInt(document.getElementById('image-query-topk').value) || 3
  };
  if (type === 'text') body.text = document.getElementById('image-query-text').value.trim();
  else body.where = document.getElementById('image-query-where').value.trim() || null;
  document.getElementById('image-query-results').innerHTML = '<div style="padding:18px;text-align:center;color:var(--muted)"><span class="loading-spin"></span>检索中...</div>';
  imageRequest('/api/image/query', {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
  }).then(renderImageQueryResults).catch(function(error) {
    document.getElementById('image-query-results').innerHTML = '<div class="image-error">检索失败：' + escapeHtml(error.message) + '</div>';
  });
}

function imageMetric(value, digits) {
  if (value == null) return '—';
  if (typeof value === 'number') return value.toFixed(digits == null ? 2 : digits);
  return escapeHtml(String(value));
}

function renderImageCard(row, rank) {
  var status = row.analysis_status || 'pending';
  var statusHtml;
  if (status !== 'ok') statusHtml = '<span class="badge badge-high">' + escapeHtml(status) + '</span>';
  else if (row.is_avatar === true) statusHtml = '<span class="badge badge-low">合规头像</span>';
  else if (row.is_avatar === false) statusHtml = '<span class="badge badge-med">不合规</span>';
  else statusHtml = '<span class="badge">待判断</span>';
  var canPreview = status !== 'download_failed' && status !== 'decode_failed';
  var preview = canPreview
    ? '<img loading="lazy" src="' + escapeHtml(row.preview_url || '') + '" alt="' + escapeHtml(row.description || row.doc_id) + '">'
    : '<div class="image-placeholder">无法预览</div>';
  var distance = row._distance == null ? '' : '<span class="image-distance">距离 ' + imageMetric(row._distance, 4) + '</span>';
  return '<div class="image-result-card">'
    + '<div class="image-preview">' + preview + '</div>'
    + '<div class="image-card-body">'
    + '<div class="image-card-title">' + (rank ? '<span class="image-rank">#' + rank + '</span>' : '')
    + escapeHtml(row.doc_id || '') + statusHtml + distance + '</div>'
    + '<div class="image-description">' + escapeHtml(row.description || '') + '</div>'
    + '<div class="image-metrics">'
    + '<span>后端 <strong>' + escapeHtml(row.analysis_backend || '—') + '</strong></span>'
    + '<span>人脸 <strong>' + imageMetric(row.face_count, 0) + '</strong></span>'
    + '<span>人脸占比 <strong>' + (row.face_area_ratio == null ? '—' : (row.face_area_ratio * 100).toFixed(1) + '%') + '</strong></span>'
    + '<span>整图清晰度 <strong>' + imageMetric(row.blur_score, 1) + '</strong></span>'
    + '<span>头像置信度 <strong>' + imageMetric(row.avatar_confidence, 2) + '</strong></span>'
    + '</div>'
    + '<div class="image-reason">' + escapeHtml(row.analysis_reason || row.analysis_error || '尚未执行合规分析') + '</div>'
    + '</div></div>';
}

function renderImageGallery(rows) {
  var ok = rows.filter(function(row) { return row.is_avatar === true; }).length;
  var rejected = rows.filter(function(row) { return row.is_avatar === false; }).length;
  var failed = rows.filter(function(row) { return row.analysis_status && row.analysis_status !== 'ok'; }).length;
  var html = '<div class="image-summary-row">'
    + '<div><strong>' + rows.length + '</strong><span>处理总数</span></div>'
    + '<div><strong style="color:var(--green)">' + ok + '</strong><span>合规头像</span></div>'
    + '<div><strong style="color:var(--amber)">' + rejected + '</strong><span>不合规</span></div>'
    + '<div><strong style="color:var(--red)">' + failed + '</strong><span>处理失败</span></div>'
    + '</div><div class="image-results-grid">';
  rows.forEach(function(row) { html += renderImageCard(row); });
  html += '</div>';
  document.getElementById('image-summary').innerHTML = html;
}

function renderImageQueryResults(data) {
  var rows = data.results || [];
  var label = data.type === 'text' ? '“' + escapeHtml(data.text || '') + '”' : escapeHtml(data.where || '全部图片');
  var html = '<div style="font-size:12px;color:var(--muted);margin-bottom:10px">'
    + label + ' · 返回 ' + rows.length + ' 条 · ' + (data.duration_s || 0) + 's</div>'
    + '<div class="image-results-grid">';
  rows.forEach(function(row, index) { html += renderImageCard(row, index + 1); });
  html += '</div>';
  if (!rows.length) html = '<div style="padding:20px;text-align:center;color:var(--muted)">没有匹配图片</div>';
  document.getElementById('image-query-results').innerHTML = html;
}

// SQL 编辑器内容持久化到 localStorage
(function() {
  var sqlEditor = document.getElementById('sql-editor');
  if (!sqlEditor) return;
  var DEFAULT_SQL = 'SELECT risk_level, COUNT(*) AS cnt\nFROM voice_analysis\nGROUP BY risk_level\nORDER BY cnt DESC';
  // 页面加载时恢复，首次访问使用默认 SQL
  var saved = localStorage.getItem('sql_editor_text');
  sqlEditor.value = saved != null ? saved : DEFAULT_SQL;
  if (!saved) localStorage.setItem('sql_editor_text', DEFAULT_SQL);
  // 输入时实时保存
  sqlEditor.addEventListener('input', function() {
    localStorage.setItem('sql_editor_text', sqlEditor.value);
  });
})();

// Init
connectWS();
loadTranscripts();
loadOverview();
setInterval(loadOverview, 30000);
