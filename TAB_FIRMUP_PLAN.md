# Tab Firmup Plan - Zipfian Benchmark Report

**Goal:** Ensure every tab tells a clear, cohesive story with proper charts and narratives.

---

## Tab Inventory & Status

### ✅ DONE - Fully Firmed Up

1. **Tab 1: Executive Summary**
   - ✅ 5 executive charts (SLA heatmap, P99 cold points, P99 curves, queries/request, tail amplification)
   - ✅ Carousel navigation
   - ✅ Parallel mode filtering fixed (w=3)
   - Status: **Ready**

2. **Tab 2: Workload Reality**
   - ✅ New tight 3-column intro
   - ✅ Fan-out breakdown chart
   - ✅ Tail amplification curve (N=10/30/50)
   - ✅ Request mix distribution chart
   - ✅ Clean narrative flow
   - Status: **Ready**

3. **Appendix Tab**
   - ✅ 7 methodology sections with code snippets
   - ✅ Defensibility checklist
   - ✅ Source code references
   - Status: **Ready**

---

### 🔧 NEEDS REVIEW - Check Content Quality

4. **Tab 3: Benchmark Design**
   - Current: Explains Zipfian distribution, key sampling, etc.
   - Action: Review for clarity and flow
   - Charts: None (text-heavy)

5. **Tab 4: Results - Latency**
   - Current: Main P99 results table at different hot%
   - Action: Check if table needs formatting improvements
   - Charts: Should reference Tab 1 executive charts

6. **Tab 5: What Dominates P99**
   - Current: Entity contribution analysis
   - Action: Check if has proper charts/heatmaps
   - Charts: Entity heatmap, bottleneck analysis

7. **Tab 6: Bin-packing**
   - Current: Query reduction analysis
   - Action: Check if has queries/request chart
   - Charts: Queries per request comparison

8. **Tab 7: Parallelism**
   - Current: Concurrency analysis
   - Action: Check if has P99 vs workers chart
   - Charts: Concurrency curve, speedup analysis

9. **Tab 8: Gantt View**
   - Current: Execution timeline visualization
   - Action: Check if Gantt chart renders
   - Charts: Serial vs Parallel Gantt

10. **Tab 9: Tail Risk**
    - Current: Tail amplification analysis
    - Action: Check if slow query log charts work
    - Charts: Slow query probability (may be placeholder)

11. **Tab 10: Recommendations**
    - Current: Next steps and production guidance
    - Action: Review recommendations are actionable
    - Charts: None (text-heavy)

---

### 📊 OLD TABS (May Not Be Needed)

12. **Overview** - Duplicate of intro?
13. **Request model** - Covered in Tab 2?
14. **Key sampling** - Covered in Appendix?
15. **Execution modes** - Covered in Tabs 6-8?
16. **Timing & logging** - Covered in Appendix?
17. **I/O measurement** - Niche, keep or merge?
18. **Results tables** - Reference material, keep
19. **All Charts** - Useful gallery, keep

---

## Recommended Actions (Priority Order)

### High Priority
1. ✅ **Tab 1: Executive Summary** - DONE
2. ✅ **Tab 2: Workload Reality** - DONE  
3. ✅ **Appendix** - DONE
4. 🔧 **Review Tabs 4-10** - Check each has proper charts and narrative

### Medium Priority
5. 🧹 **Consolidate/Remove duplicate tabs** - Merge Overview/Request-model into Tab 2
6. 🎨 **Ensure consistent styling** - All tabs use same card/grid patterns

### Low Priority
7. 📝 **Add more code snippets** - If engineers request specific implementation details
8. 🔗 **Cross-tab navigation** - Add "See Tab X for details" links

---

## Next Steps

**While benchmark runs (2-3 hours):**
1. Review Tab 3 (Benchmark Design) - is it clear?
2. Review Tab 4 (Results) - is table formatted well?
3. Review Tabs 5-10 - ensure each has charts + narrative
4. Consider removing redundant old tabs

**After benchmark completes:**
1. Re-run viz job to get complete parallel mode data
2. Verify all Tab 1 charts show parallel w=3 data
3. Verify Tab 9 (Tail Risk) shows real slow query data instead of placeholder

---

## Questions to Resolve

1. **Should we keep all 19 tabs?** Too many tabs = navigation confusion
2. **Which tabs can be merged/removed?** Overview, Request model, Key sampling, Execution modes, Timing likely redundant
3. **What's the ideal tab order?** Executive → Workload → Design → Results → Deep dives → Recommendations → Appendix?
