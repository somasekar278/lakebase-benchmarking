# 🎯 DynamoDB Discovery Questions - Customer Meeting Prep

**Meeting Objective**: Gather precise DynamoDB details to ensure apples-to-apples Lakebase comparison

---

## 📋 **Pre-Meeting Checklist**

Bring with you:
- [ ] Your Lakebase benchmark results (P99=41ms, etc.)
- [ ] Cost calculator (`COST_METHODOLOGY.md`)
- [ ] Laptop with `calculate_serving_costs.py` ready
- [ ] This questionnaire
- [ ] Note-taking device/app

---

## 1️⃣ **DynamoDB Cost Breakdown** 💰

### Current Costs (Verify Your Numbers)

**Q1: What is your TOTAL monthly DynamoDB bill?**
```
Current assumption: $2.25M/month ($75K/day)
Actual: $_____________

Get exact number from AWS Cost Explorer
```

**Q2: How is the $1,500/table/day calculated?**
```
[ ] Actual measured cost per table
[ ] Estimated/averaged across 50 tables
[ ] Other: _____________

Can they share AWS Cost & Usage Report?
```

**Q3: What capacity mode are you using?**
```
[ ] On-Demand (pay-per-request)
[ ] Provisioned (pre-allocated RCU/WCU)
[ ] Auto-scaling provisioned

WHY IT MATTERS:
- On-Demand: Higher per-request cost but no waste
- Provisioned: Cheaper per-request but pay for unused capacity
```

### Hidden/Additional Costs

**Q4: Does the $1,500/day include ALL costs or just table capacity?**
```
Check if they're also paying for:
[ ] Data transfer OUT ($0.09/GB for internet, $0.01/GB cross-region)
[ ] Backup storage ($0.10/GB-month for on-demand backups)
[ ] Point-in-time recovery (PITR) ($0.20/GB-month)
[ ] Global tables replication ($1.875 per million replicated writes)
[ ] DynamoDB Streams ($0.02 per 100K reads)
[ ] Reserved capacity purchases (upfront payments)

ACTION: Ask for AWS Cost & Usage Report filtered by service
```

**Q5: Are you using DynamoDB Accelerator (DAX)?**
```
[ ] Yes - Cost: $______/month, Cache hit ratio: _____%
[ ] No

WHY IT MATTERS:
- DAX adds significant cost ($200-800/month per node)
- If using DAX, their "DynamoDB latency" includes cache
- We need to compare Lakebase WITH cache vs DynamoDB WITH DAX
```

**Q6: Any reserved capacity commitments?**
```
- Upfront payments already made: $_____________
- Contract end dates: _____________
- Early termination penalties: $_____________
```

---

## 2️⃣ **Performance Baseline** ⚡

### Actual Measurements

**Q7: Where did the "P99 = 79ms, Avg = 30ms" come from?**
```
[ ] CloudWatch metrics (accurate)
[ ] Application-side measurement (includes network)
[ ] Load test results (may not reflect production)
[ ] Vendor claim (unreliable)

ASK FOR: Screenshots of CloudWatch "GetItem/Query duration" metrics
```

**Q8: What percentiles do you ACTUALLY monitor in production?**
```
[ ] P50 (median)
[ ] P95
[ ] P99
[ ] P99.9
[ ] P99.99

ACTION: Match these exact percentiles in Lakebase tests
```

**Q9: What is your latency SLA?**
```
- Internal SLA: P__ < ___ms
- Customer-facing SLA: P__ < ___ms

WHY IT MATTERS: This is the bar Lakebase needs to clear
```

**Q10: What is your current P99 during PEAK hours?**
```
- Best case (off-peak): ___ms
- Peak (busy hours): ___ms
- Worst case (incidents): ___ms

WARNING: If they only measure off-peak, P99 may be much worse in reality
```

### Workload Characteristics

**Q11: What is your sustained queries-per-second (QPS)?**
```
- Average QPS: _______
- Peak QPS: _______
- Off-peak QPS: _______
- Time of peak: _______ (timezone)

ACTION: Configure Lakebase test to match PEAK QPS
```

**Q12: What percentage of queries are:**
```
- Point lookups (GetItem): _____%
- Range queries (Query): _____%
- Scans: _____%
- Batch operations: _____%

WHY IT MATTERS: Different query types have different latencies
```

**Q13: How many tables do you query PER REQUEST?**
```
Current assumption: 50 tables (all tables)
Actual: _____ tables per request

CRITICAL: If they only query 10-20 tables at a time, adjust Lakebase tests!
```

**Q14: What is your typical item size?**
```
- Average item size: _____ KB
- Largest item size: _____ KB
- Total data size: _____ GB (per table)

WHY IT MATTERS: Larger items = more data transfer = slower queries
```

---

## 3️⃣ **DynamoDB Configuration** ⚙️

### Table Design

**Q15: What consistency model are you using?**
```
[ ] Eventually consistent reads (default, cheaper, faster)
[ ] Strongly consistent reads (2x RCU cost, slightly slower)

WHY IT MATTERS:
- Eventually consistent: 3.9ms average
- Strongly consistent: 11.6ms average
- Lakebase is ALWAYS strongly consistent
```

**Q16: How are your tables structured?**
```
- Partition key: _________ (e.g., hash_key)
- Sort key: _________ (if any)
- Number of items per partition: _________

WHY IT MATTERS: Hot partitions cause throttling and high latency
```

**Q17: Are you using Global Secondary Indexes (GSI)?**
```
[ ] Yes - Count: _____, Used in queries: _____%
[ ] No

WARNING: GSI queries are slower than base table queries
```

**Q18: What read/write capacity units (RCU/WCU) are provisioned?**
```
(If using provisioned capacity)

Per table:
- Read Capacity Units: _______
- Write Capacity Units: _______
- Auto-scaling enabled: [ ] Yes [ ] No
- Auto-scaling range: ______ to ______ RCUs

ACTION: Calculate actual utilization % to find waste
```

### Data Characteristics

**Q19: What is your TOTAL data size across all 50 tables?**
```
Current assumption: 1.5 TB
Actual: _______ TB

CRITICAL: Storage costs scale linearly in Lakebase
```

**Q20: What is your data growth rate?**
```
- Monthly data growth: _______ GB/month
- Annual data growth: _______ TB/year

ACTION: Project 3-year storage costs for both systems
```

**Q21: What is your data churn rate?**
```
- % of data updated daily: _______%
- % of data updated monthly: _______%

WHY IT MATTERS: Affects PITR storage costs in Lakebase
```

---

## 4️⃣ **Architecture & Access Patterns** 🏗️

### Application Integration

**Q22: Where is your application running?**
```
[ ] Same AWS region as DynamoDB
[ ] Different AWS region (which: _______)
[ ] Multi-region
[ ] On-premises (hybrid)

WHY IT MATTERS: Network latency matters for comparison
```

**Q23: What is your network RTT to DynamoDB?**
```
- Typical round-trip time: _____ ms
- 95th percentile RTT: _____ ms

ACTION: Measure Databricks → Lakebase RTT and compare
```

**Q24: How do you handle caching?**
```
[ ] Application-level cache (Redis, Memcached)
[ ] DynamoDB Accelerator (DAX)
[ ] No caching

If caching:
- Cache hit rate: _______%
- Cache size: _______ GB
- Cache cost: $_______ /month
```

**Q25: What SDK/library are you using?**
```
- Language: _________ (Python, Java, Node.js, etc.)
- AWS SDK version: _________
- Connection pooling: [ ] Yes [ ] No
- Retry logic: [ ] Exponential backoff [ ] Custom [ ] None
```

### Query Patterns

**Q26: What does a "feature serving request" look like?**
```
Walk through a REAL example:

1. Application receives request with: _________
2. Query tables in this order: _________
3. Query type per table: _________
4. Join/aggregate results: [ ] Yes [ ] No
5. Return to application: _________

ACTION: Replicate EXACT pattern in Lakebase benchmark
```

**Q27: Do you batch requests?**
```
[ ] Yes - Batch size: _____ items
[ ] No - Individual GetItem calls

WHY IT MATTERS: BatchGetItem can fetch 100 items in one call
```

**Q28: Are queries sequential or parallel?**
```
[ ] Sequential (one table at a time) = 50 × latency
[ ] Parallel (all tables at once) = max(latency)

CRITICAL: This drastically affects total latency!
```

---

## 5️⃣ **Operational Aspects** 🔧

### Monitoring & Alerting

**Q29: What metrics do you actively monitor?**
```
[ ] Latency percentiles
[ ] Throttled requests
[ ] Error rate
[ ] Consumed capacity
[ ] System errors

ACTION: Set up same monitoring for Lakebase
```

**Q30: What is your current error/throttle rate?**
```
- Throttled read requests: _______/day
- Throttled write requests: _______/day
- 4xx errors: _______/day
- 5xx errors: _______/day

WHY IT MATTERS: High throttling = under-provisioned = hidden latency
```

**Q31: Have you experienced any incidents?**
```
- Outages in past 12 months: _______
- P99 degradation events: _______
- Throttling events: _______

ACTION: Compare to Lakebase uptime SLA
```

### Backup & DR

**Q32: What is your backup/DR strategy?**
```
[ ] Point-in-time recovery (PITR) - Window: _____ days
[ ] On-demand backups - Frequency: _______
[ ] Cross-region replication - Regions: _______
[ ] Export to S3 - Frequency: _______

TOTAL BACKUP COST: $_______ /month
```

**Q33: What is your RTO/RPO requirement?**
```
- Recovery Time Objective (RTO): _____ minutes
- Recovery Point Objective (RPO): _____ minutes

ACTION: Verify Lakebase PITR meets requirements
```

---

## 6️⃣ **Business Context** 💼

### Decision Criteria

**Q34: What is your #1 pain point with DynamoDB?**
```
[ ] Cost (too expensive)
[ ] Performance (too slow)
[ ] Complexity (hard to manage)
[ ] Vendor lock-in
[ ] Scaling issues
[ ] Other: _________
```

**Q35: What would make you switch to Lakebase?**
```
[ ] X% cost savings (X = ______)
[ ] X% latency improvement (X = ______)
[ ] Better developer experience
[ ] Unified platform with Databricks
[ ] SQL interface
[ ] Other: _________
```

**Q36: What is your decision timeline?**
```
- Decision date: _________
- Migration start: _________
- Migration complete: _________

ACTION: Align Lakebase POC timeline
```

**Q37: Who are the stakeholders?**
```
- Technical decision maker: _________
- Budget owner: _________
- Executive sponsor: _________
- End users: _________
```

### Risk & Concerns

**Q38: What concerns do you have about migrating?**
```
[ ] Migration effort/complexity
[ ] Data consistency during migration
[ ] Application code changes required
[ ] Team training
[ ] Vendor maturity (Lakebase is new)
[ ] Performance regression risk
[ ] Lock-in to Databricks

ACTION: Address each concern in your proposal
```

**Q39: What would be a deal-breaker?**
```
[ ] Latency > _____ ms
[ ] Cost > $_______ /month
[ ] Migration time > _____ weeks
[ ] Downtime > _____ minutes
[ ] Other: _________
```

---

## 7️⃣ **Technical Deep-Dive** 🔬

### For Engineers in the Room

**Q40: Can you share CloudWatch dashboard/metrics?**
```
Specifically:
- GetItem/Query/BatchGetItem latency distribution
- ConsumedReadCapacityUnits over time
- ThrottledRequests over time
- SystemErrors and UserErrors

ACTION: Compare exact same metrics for Lakebase
```

**Q41: Can you share a sample of your data schema?**
```
- DDL or JSON schema of 1-2 tables
- Sample items (anonymized)

ACTION: Load representative data into Lakebase
```

**Q42: Can you share AWS Cost & Usage Report?**
```
Filter by:
- Service: DynamoDB
- Usage type: All
- Time period: Last 3 months

LOOK FOR:
- TimedStorage-ByteHrs (storage costs)
- PayPerRequestThroughput (on-demand)
- ProvisionedThroughput (provisioned)
- DataTransfer-Out-Bytes (network egress)
```

**Q43: What percentage of your queries hit hot keys?**
```
- Top 1% of keys → ____% of queries (Zipfian)
- Even distribution across all keys
- Other pattern: _________

ACTION: Replicate exact distribution in Lakebase test
```

---

## 🎯 **Post-Meeting Action Items**

### Immediate (Same Day)
- [ ] Send thank-you email with meeting notes
- [ ] Request any missing data (Cost reports, CloudWatch screenshots)
- [ ] Update `COST_METHODOLOGY.md` with actual numbers
- [ ] Adjust Lakebase test configuration to match their workload

### Within 24 Hours
- [ ] Re-run Lakebase benchmarks with correct parameters
- [ ] Update `calculate_serving_costs.py` with actual DynamoDB costs
- [ ] Generate updated executive dashboard
- [ ] Calculate precise ROI

### Within 48 Hours
- [ ] Send preliminary cost comparison
- [ ] Propose POC plan (if interested)
- [ ] Schedule follow-up demo

---

## 📊 **Red Flags to Watch For**

### Cost Red Flags 🚩

```
❌ "We don't track cost per table" 
   → They might not know actual costs
   
❌ "The bill varies a lot month-to-month"
   → Under-provisioned = throttling = hidden latency
   
❌ "We're using reserved capacity"
   → Sunk cost fallacy - don't let this block migration
   
❌ "We also pay for [long list of other services]"
   → Total AWS spend may be much higher than stated
```

### Performance Red Flags 🚩

```
❌ "Our P99 is usually around 79ms"
   → "Usually" = not measured precisely
   
❌ "We only measure latency at the application layer"
   → Includes network, retries, timeouts
   
❌ "We have a cache in front of DynamoDB"
   → Their "DynamoDB latency" is really "cache latency"
   
❌ "Latency spikes during deployments/peak hours"
   → Capacity issues = need to compare at same load
```

### Architecture Red Flags 🚩

```
❌ "We fan out to 50 tables sequentially"
   → 50 × 30ms = 1,500ms total!
   → Lakebase can join in one query
   
❌ "We sometimes get throttled"
   → Under-provisioned = actual latency worse than stated
   
❌ "We query different tables for different requests"
   → Need to benchmark multiple query patterns
```

---

## 💡 **Pro Tips for the Meeting**

### Do's ✅

1. **Listen more than you talk** - Let them explain their pain points
2. **Take detailed notes** - Every number matters for comparison
3. **Ask for screenshots/dashboards** - Don't rely on memory
4. **Validate assumptions** - "Just to confirm, you said..."
5. **Be honest about Lakebase limitations** - Builds trust
6. **Focus on their problems** - Not just features
7. **Get concrete numbers** - Not "around" or "approximately"

### Don'ts ❌

1. **Don't oversell** - Let the numbers speak
2. **Don't dismiss their concerns** - Address them seriously
3. **Don't assume** - Verify every detail
4. **Don't compare apples to oranges** - Match their exact workload
5. **Don't forget to ask about DAX** - Critical for cost comparison
6. **Don't rush** - Take time to understand their setup

---

## 📝 **Meeting Template**

```
CUSTOMER: _____________________
DATE: _________________________
ATTENDEES: ____________________

CURRENT STATE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- DynamoDB monthly cost: $__________
- Number of tables: __________
- Latency P99: __________ ms
- Throughput (QPS): __________
- Pain points: 
  1. __________
  2. __________
  3. __________

REQUIREMENTS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Must-have latency: P__ < ____ms
- Must-have throughput: ______ QPS
- Must-have availability: ______%
- Deal-breakers: __________

CONFIGURATION DETAILS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Capacity mode: __________
- Consistency model: __________
- Caching (DAX): [ ] Yes [ ] No
- Data size: __________ TB
- Query pattern: [ ] Sequential [ ] Parallel
- Tables per request: __________

NEXT STEPS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- [ ] Customer provides: __________
- [ ] We deliver: __________
- [ ] Follow-up meeting: __________
```

---

## 🚀 **Expected Outcomes**

After this meeting, you should have:

1. ✅ **Exact DynamoDB costs** (not estimated)
2. ✅ **Measured performance metrics** (not vendor claims)
3. ✅ **Complete workload profile** (QPS, query types, data size)
4. ✅ **Configuration details** (capacity mode, consistency, caching)
5. ✅ **Decision criteria** (what would make them switch)
6. ✅ **Concerns addressed** (what could block migration)

Then you can:
- Run **precise Lakebase benchmarks** matching their workload
- Calculate **exact cost comparison** with real numbers
- Present **data-driven recommendation** with ROI

---

## 📋 **Key Metrics Summary Sheet**

Use this as a quick reference during the meeting:

| Category | Metric | Their Answer | Lakebase Equivalent |
|----------|--------|--------------|---------------------|
| **Cost** | Monthly bill | $_________ | $_________ |
| | Cost/table/day | $_________ | $_________ |
| | Hidden costs | $_________ | $_________ |
| **Performance** | P99 latency | ______ ms | ______ ms |
| | Peak QPS | ______ | ______ |
| | Error rate | ______% | ______% |
| **Data** | Total size | ______ TB | ______ TB |
| | Growth rate | ______ GB/mo | ______ GB/mo |
| | Item size | ______ KB | ______ KB |
| **Config** | Capacity mode | _________ | Autoscaling |
| | Consistency | _________ | Strong |
| | Caching | _________ | Built-in |

---

## 🎯 **Critical Questions (Don't Leave Without Answers)**

These 10 questions are MUST-HAVE:

1. ✅ **Total monthly DynamoDB cost** (exact number)
2. ✅ **P99 latency during peak hours** (from CloudWatch)
3. ✅ **Peak QPS** (sustained, not burst)
4. ✅ **Tables queried per request** (10? 20? 50?)
5. ✅ **Sequential or parallel queries?** (critical for latency)
6. ✅ **Using DAX cache?** (yes/no and cache hit rate)
7. ✅ **Consistency model** (eventual or strong)
8. ✅ **Total data size** (across all tables)
9. ✅ **Latency SLA** (what's the bar to clear)
10. ✅ **Decision timeline** (when do they need answer)

---

**Good luck with your meeting! 🎯**

**Come back with these answers and we'll build an unbeatable business case!**
