# RPC Mode Chart Impact Assessment

## Executive Summary

**Total Charts:** 18 active charts in report  
**Affected Charts:** 12 require changes  
**Unchanged Charts:** 6 (mode-independent or Tab 2)  

**Estimated Complexity:** Medium (4-6 hours total work)

---

## 🔴 HIGH PRIORITY - Core Executive Charts (Tab 1)

### ✅ Chart 1: P99 at Cold Points (0% hot)
**File:** `zipfian_benchmark_visuals.py` (lines 1256-1367)  
**Current Modes:** Serial, Binpacked, Parallel (w=3)  
**Change Required:** ✅ **YES - Add RPC as 4th bar**

**Impact:**
- Add `'rpc_request_json'` to `modes` list (line 1289)
- Add mode label: `'RPC\n(1 query)'` 
- Add color: `'rpc_request_json': '#00D4FF'` (Checkout cyan)
- Adjust x_positions to accommodate 4 bars
- Update 79ms reference line text position (line 1353) from x=2.5 to x=3.5

**Complexity:** Low (10 min)  
**Risk:** Low - straightforward bar addition

---

### ✅ Chart 2: Queries Per Request
**File:** `zipfian_benchmark_visuals.py` (lines 1370-1453)  
**Current Modes:** Serial (30), Binpacked (10), Parallel (10)  
**Change Required:** ✅ **YES - Add RPC showing 1 query**

**Impact:**
- Add RPC to modes loop (line 1378)
- Fallback value: `1 if mode == 'rpc_request_json' else ...`
- Add annotation showing `-90%` (30→1) for Serial→RPC
- Adjust bar width/spacing for 4 bars
- **This will be the HERO stat: 30 → 1 query (-97%)**

**Complexity:** Low (15 min)  
**Risk:** Low - critical storytelling moment

---

### ✅ Chart 3: Cold Penalty (Resilience Ratio)
**File:** `zipfian_benchmark_visuals.py` (lines 1457-1553)  
**Current Modes:** Serial, Binpacked, Parallel  
**Change Required:** ✅ **YES - Add RPC penalty ratio**

**Impact:**
- Add RPC to modes loop (line 1465)
- Calculate P99 @ 100% hot vs 0% hot for RPC
- Expected ratio: ~1.8x (better than Serial at 2.5x, similar to Parallel)
- Add 4th bar with proper coloring

**Complexity:** Low (10 min)  
**Risk:** Low - identical logic to existing modes

---

### ✅ Chart 4: SLA Heatmap (0%, 10% hot)
**File:** `zipfian_benchmark_visuals.py` (lines 1568-1709)  
**Current Modes:** Serial, Binpacked, Parallel (2×3 grid)  
**Change Required:** ✅ **YES - Expand to 2×4 grid**

**Impact:**
- Add `'rpc_request_json'` to modes list (line 1578)
- Update `mode_labels_short` to include `'RPC\n(1 query)'`
- Adjust cell_width calculation for 4 columns (line 1624)
- Update ax.set_xlim from `3 * cell_width` to `4 * cell_width` (line 1627)
- Add 4th column to xticks (line 1671)
- **Expected result: RPC will show GREEN (<79ms) at both 0% and 10% hot**

**Complexity:** Medium (20 min)  
**Risk:** Medium - layout math needs careful adjustment

---

### ✅ Chart 5: Entity P99 Composition
**File:** `zipfian_benchmark_visuals.py` (lines 1713-1822)  
**Current:** Parallel mode only @ 0% hot  
**Change Required:** 🟡 **OPTIONAL - Could show RPC comparison**

**Impact:**
- Currently shows only parallel mode entity breakdown
- Could add RPC mode as second panel for comparison
- **Recommendation: SKIP for v5.4 - focus on top 4 charts first**

**Complexity:** Medium (30 min)  
**Risk:** Low - optional enhancement

---

### ✅ Chart 6: Tail Amplification Probability
**File:** `zipfian_benchmark_visuals.py` (lines 1824-1990)  
**Current:** Mode-agnostic theoretical curve  
**Change Required:** ❌ **NO - Mode-independent**

**Impact:** None - this is a theoretical tail amplification curve based on N (query count), not specific modes

---

## 🟡 MEDIUM PRIORITY - Diagnostic Charts (Tab 4)

### ✅ Chart 7: Request Latency ECDF
**File:** `zipfian_benchmark_visuals.py` (lines 2267-2362)  
**Current:** Serial mode only (10% vs 0% hot)  
**Change Required:** 🟡 **OPTIONAL - Could add RPC comparison**

**Impact:**
- Currently shows serial mode ECDF at 2 hot% levels
- Could add RPC as second panel to show distribution improvement
- **Recommendation: Add RPC for dramatic visual contrast**

**Complexity:** Medium (25 min)  
**Risk:** Low

---

### ✅ Chart 8: Cold Penalty Quantification
**File:** `zipfian_benchmark_visuals.py` (lines 2364-2436)  
**Current:** Serial mode only  
**Change Required:** 🟡 **OPTIONAL - Could add RPC**

**Impact:**
- Shows latency by # hot entities (0/1/2/3)
- Could add RPC mode as comparison
- **Recommendation: SKIP - sufficient to show serial mode**

**Complexity:** Medium (20 min)  
**Risk:** Low

---

### ✅ Chart 9: Entity Contribution Breakdown
**File:** `zipfian_benchmark_visuals.py` (lines 2438-2522)  
**Current:** Serial mode stacked bars across hot%  
**Change Required:** 🟡 **OPTIONAL**

**Impact:**
- Could add RPC mode panel
- **Recommendation: SKIP - entity contribution pattern similar across modes**

**Complexity:** Medium (25 min)  
**Risk:** Low

---

### ✅ Chart 10: Bin-packing Effectiveness (Strategy Payoff)
**File:** `zipfian_benchmark_visuals.py` (lines 2524-2668)  
**Current:** Shows Serial, Binpacked, Parallel tail improvement  
**Change Required:** ✅ **YES - Add RPC as final bar**

**Impact:**
- Add RPC mode to the tail improvement chart
- Show P99 reduction: Serial → RPC (dramatic ~40-50% improvement)
- This chart shows "Strategy Payoff" so RPC is the **ultimate payoff**
- Add 4th bar with cyan color

**Complexity:** Low (15 min)  
**Risk:** Low

---

## 🟢 LOW PRIORITY - Supporting Charts

### ✅ Chart 11: Gantt Visualization (Tab 5)
**File:** `zipfian_benchmark_visuals.py` (lines 840-1127)  
**Current:** Serial vs Parallel overlap  
**Change Required:** 🟡 **OPTIONAL - Could add RPC panel**

**Impact:**
- RPC mode: Single bar representing single server-side call
- Would show "no overlap needed" (all server-side)
- **Recommendation: SKIP - Gantt is about parallelism, RPC is different paradigm**

**Complexity:** High (45 min)  
**Risk:** Medium - conceptually different

---

### ✅ Chart 12: Workers Sweep (Concurrency)
**File:** `zipfian_benchmark_visuals.py` (lines 560-715)  
**Current:** P99 vs worker count (w=1,2,3,4)  
**Change Required:** 🟡 **OPTIONAL - Add RPC baseline**

**Impact:**
- Add horizontal reference line for RPC mode (w=N/A, single value)
- Shows RPC as "no workers needed" baseline
- **Recommendation: ADD - shows RPC beats even parallel w=3**

**Complexity:** Low (10 min)  
**Risk:** Low

---

### ✅ Chart 13: P99 vs Skew (Main curves chart)
**File:** `zipfian_benchmark_visuals.py` (lines 190-430)  
**Current:** Multiple mode curves across hot% (0-100%)  
**Change Required:** ✅ **YES - Add RPC curve**

**Impact:**
- Add RPC mode to line plot
- Filter: `df[df['fetch_mode'] == 'rpc_request_json']`
- Add curve label: "RPC (1 query)"
- Use cyan color: `'#00D4FF'`
- **This is a HERO chart showing RPC dominance**

**Complexity:** Low (10 min)  
**Risk:** Low

---

## ❌ NO CHANGE REQUIRED - Mode-Independent Charts

### Chart 14-16: Workload Reality (Tab 2)
**Files:** Fanout breakdown, Amplification curve, Request mix  
**Change Required:** ❌ **NO**  

**Reason:** These charts explain the *problem* (30 lookups, tail amplification, request mix), not the *solutions*. They are mode-agnostic by design.

### Chart 17-18: Cost Charts (If present)
**Change Required:** 🟡 **TBD - Depends on cost model**

**Impact:** If cost charts exist, RPC should show:
- Queries/request: 1 (lowest)
- Network round-trips: 1 (lowest)
- Connection pool pressure: lowest
- BUT: Server-side CPU higher (UNION ALL queries server-side)

---

## 📊 Recommended Implementation Priority

### Phase 1: Essential Updates (Must Do for v5.4)
1. ✅ **Chart 1: P99 Cold Points** - Add 4th bar (10 min)
2. ✅ **Chart 2: Queries Per Request** - Hero stat 30→1 (15 min)
3. ✅ **Chart 3: Cold Penalty** - Add 4th bar (10 min)
4. ✅ **Chart 4: SLA Heatmap** - Expand to 2×4 grid (20 min)
5. ✅ **Chart 13: P99 vs Skew Curves** - Add RPC line (10 min)

**Total Phase 1:** ~65 minutes

### Phase 2: High-Impact Additions (Recommended)
6. ✅ **Chart 10: Strategy Payoff** - Add RPC final bar (15 min)
7. ✅ **Chart 12: Workers Sweep** - Add RPC baseline (10 min)
8. ✅ **Chart 7: Latency ECDF** - Add RPC comparison panel (25 min)

**Total Phase 2:** ~50 minutes

### Phase 3: Optional Enhancements (Nice to Have)
9. 🟡 **Chart 8: Cold Penalty Quantification** - Add RPC (20 min)
10. 🟡 **Chart 11: Gantt** - Add RPC panel (45 min)

**Total Phase 3:** ~65 minutes

---

## 🎨 Visual Design Recommendations

### Color Scheme (for RPC mode)
- **Primary:** `#00D4FF` (Checkout cyan) - Use for RPC bars/lines
- **Secondary:** `#7c3aed` (Violet) - Accent if needed
- **Rationale:** Distinct from Serial (gray-blue), Binpacked (light blue), Parallel (bright blue)

### Label Conventions
- **Short:** `"RPC"` or `"RPC\n(1 query)"`
- **Long:** `"RPC Request JSON\n(Single server call)"`
- **Queries:** Always show as `1.0` or `1`

### Legend Order
1. Serial (30 queries) - Baseline
2. Binpacked (10 queries) - First optimization
3. Parallel (10 queries, w=3) - Second optimization
4. **RPC (1 query)** - Ultimate optimization 🏆

---

## 🧪 Testing Checklist

After implementing RPC mode in charts:

- [ ] All 4 modes appear in Chart 1 (Cold Points)
- [ ] Chart 2 shows dramatic 30→1 query reduction
- [ ] Chart 4 heatmap has 4 columns (not clipped)
- [ ] RPC line appears in P99 curves chart
- [ ] RPC color is consistent (cyan) across all charts
- [ ] No overlapping labels or axis issues
- [ ] All charts maintain professional spacing
- [ ] Legend entries are in correct order
- [ ] Reference lines (79ms SLA) still visible
- [ ] Chart titles/subtitles updated if they reference "3 modes"

---

## 📝 Code Patterns to Follow

### Adding RPC to mode lists:
```python
modes = ['serial', 'binpacked', 'binpacked_parallel', 'rpc_request_json']

mode_labels = {
    'serial': 'Serial\n(30 queries)',
    'binpacked': 'Bin-packed\n(10 queries)',
    'binpacked_parallel': 'Parallel\n(10 queries, 3 workers)',
    'rpc_request_json': 'RPC\n(1 query)'
}

mode_colors = {
    'serial': '#94A3B8',
    'binpacked': '#60A5FA',
    'binpacked_parallel': '#357FF5',
    'rpc_request_json': '#00D4FF'
}
```

### Filtering RPC data:
```python
rpc_df = df[df['fetch_mode'] == 'rpc_request_json']
if len(rpc_df) > 0:
    p99_val = rpc_df.iloc[0]['p99_ms']
    queries = rpc_df.iloc[0].get('queries_per_request', 1.0)
```

### Fallback for missing RPC data:
```python
if mode == 'rpc_request_json':
    queries = row.get('queries_per_request', 1.0)  # Always 1 for RPC
```

---

## 🚨 Common Pitfalls to Avoid

1. **Don't forget to update `x_positions` range** when adding 4th bar
2. **Cell width calculations** in heatmap need adjustment for 4 columns
3. **Reference line positions** (e.g., 79ms SLA) may need shifting right
4. **Legend positioning** may need adjustment with 4 items
5. **Annotation arrows** (e.g., reduction %) need recalculation for 30→1
6. **Chart width** may need increase from 14 to 16 inches for readability

---

## 📈 Expected Visual Impact

**Before (v5.3):** 3-mode comparison shows incremental improvements  
**After (v5.4):** RPC mode shows **dramatic leap** in final optimization

**Key Story:** 
- Serial → Binpacked: -67% queries (30 → 10)
- Binpacked → Parallel: Same queries, overlapped execution
- Parallel → RPC: **-90% queries (10 → 1), single server call** 🏆

**Narrative:** "We've exhausted client-side optimizations. RPC is the server-side breakthrough."

---

## 🎯 Success Criteria

After all changes:
- ✅ RPC appears in all mode-comparison charts
- ✅ RPC consistently uses cyan color (#00D4FF)
- ✅ RPC shows best P99 performance at 0% and 10% hot
- ✅ Queries per request clearly shows 1.0 for RPC
- ✅ No visual artifacts (overlapping text, clipped bars)
- ✅ Professional layout maintained
- ✅ All charts render at 150 DPI with consistent sizing

---

## 📦 Files to Modify

1. **`notebooks/zipfian_benchmark_visuals.py`** - All chart generation code
   - Lines 1256-1367: Chart 1 (Cold Points)
   - Lines 1370-1453: Chart 2 (Queries Per Request)
   - Lines 1457-1553: Chart 3 (Cold Penalty)
   - Lines 1568-1709: Chart 4 (SLA Heatmap)
   - Lines 190-430: Chart 13 (P99 Curves)
   - Lines 2524-2668: Chart 10 (Strategy Payoff)
   - Lines 560-715: Chart 12 (Workers Sweep)

2. **`report_template.html`** - No changes needed (placeholders already exist)

---

**Total Estimated Time:** 
- Phase 1 (Essential): 65 min
- Phase 2 (Recommended): 50 min
- **Total for production-ready report:** ~2 hours

**Recommended approach:** Implement Phase 1 first, test mini benchmark, then add Phase 2 charts.
