# Kannan Natarajan: Engineering High-Scale Streaming Systems

## Project Profile: Real-Time Fraud Detection & Analytics Platform

### Executive Summary

I engineered and scaled a real-time fraud detection platform processing millions of daily
transaction events across multiple banking channels. By leveraging event-driven
architecture, the system applied complex behavioral rules on the fly to generate immediate
fraud signals for analysts and automated decision systems.

- **Business Impact:** Contributed directly to approximately $5 million in annual fraud-loss
  reduction.
- **Core Tech Stack:** Apache Flink, Apache Kafka, Apache Spark, Snowflake, Spring Boot.

---

### 1. Ensuring Signal Accuracy in Distributed Chaos

**Tradeoff: Event-Time Accuracy vs. Alert Latency**

**The Business Risk:** Banking transactions frequently arrive at the streaming platform out of
order due to upstream network delays. If we processed fraud rules based strictly on when
the system received the event (Processing Time), late-arriving transactions would trigger
false positives or cause us to miss coordinated velocity attacks entirely.

**The Engineering Strategy:** I implemented Flink's Event-Time Processing paired with
Bounded Watermarks. This ensured the fraud engine evaluated customer behavior based
on when the transaction actually occurred, not when it hit our Kafka topic.

```java
// Bounded out-of-order interval accommodates network lag without
// stalling the pipeline
WatermarkStrategy<Transaction> watermarkStrategy =
    WatermarkStrategy
        .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner((event, timestamp) ->
            event.getTransactionTimestamp());

transactions
    .assignTimestampsAndWatermarks(watermarkStrategy)
    .keyBy(Transaction::getCustomerId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .process(new FraudDetectionProcessFunction());
```

**The Production Impact:** We achieved highly accurate behavioral pattern matching. By
bounding the watermark delay (e.g., 10 seconds), we absorbed normal source-system lag
without artificially delaying critical alerts to the fraud operations team.

> 💡 **Key Takeaway:** "We chose event-time processing because network arrival order is
> inherently unreliable. We traded a few seconds of pipeline latency to guarantee the
> mathematical accuracy of our fraud patterns."

---

### 2. Intelligent Scaling & Resource Management

**Tradeoff: Throughput/Parallelism vs. Infrastructure Cost**

**The Business Risk:** Processing millions of events daily requires significant compute. A
naive approach, cranking up Flink parallelism globally across the entire pipeline, wastes
expensive cloud resources and masks underlying architectural bottlenecks.

**The Engineering Strategy:** Instead of blindly scaling, I adopted a metrics-driven scaling
strategy. I monitored Kafka consumer lag, Flink backpressure, CPU/Memory utilization, and
operator latency to identify precise bottlenecks, scaling only the operators that required it.

```java
// Targeted scaling: Increasing parallelism only where compute is bound
env.setParallelism(2); // Baseline for lightweight operations

transactions
    .keyBy(Transaction::getCustomerId)
    .process(new FraudDetectionProcessFunction())
    .setParallelism(8); // Scaled specifically for the heavy rule-evaluation operator
```

**The Production Impact:** The platform effortlessly handled peak transaction traffic while
maintaining strict cost controls. This granular approach also made operational
troubleshooting much faster, allowing us to isolate whether a slowdown was due to Kafka, a
specific Flink operator, or a downstream REST API sink.

> 💡 **Key Takeaway:** "I don't scale systems blindly. By monitoring backpressure and
> operator latency, we isolated our bottlenecks and scaled only the specific components
> that needed it, balancing high throughput with cloud cost efficiency."

---

### 3. Designing Resilient Stateful Pipelines

**Tradeoff: Rich Behavioral Context vs. Recovery Speed**

**The Business Risk:** Fraud detection relies on historical context (e.g., "Is this a sudden spike
in frequency for this specific account?"). Storing massive amounts of historical state inside
Flink improves detection but bloats checkpoints, increases memory costs, and creates
painfully slow recovery times during system failures.

**The Engineering Strategy:** I decoupled state storage based on the decision window. I
utilized Flink's Keyed State with Time-To-Live (TTL) strictly for immediate, short-term
behavioral context. All long-term historical analytics were offloaded to Snowflake.

```java
// State TTL ensures Flink only holds decision-critical data,
// preventing state bloat
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .build();

ValueStateDescriptor<FraudContext> descriptor =
    new ValueStateDescriptor<>("fraud-context", FraudContext.class);
descriptor.enableTimeToLive(ttlConfig);

// Fast, lightweight checkpoints
env.enableCheckpointing(60000);
```

**The Production Impact:** The streaming pipeline retained exactly the context required for
sub-second fraud decisions. Because we didn't treat Flink as a long-term database, memory
footprint stayed low, checkpointing completed in seconds, and system recovery was
virtually instantaneous.

> 💡 **Key Takeaway:** "More state means better fraud detection, but heavier checkpoints.
> We kept Flink lean by storing only decision-critical, short-term state and pushing deep
> historical analysis to Snowflake."

---

### 4. Safe Model Rollouts with Staging & A/B Testing

**Tradeoff: Faster Model Iteration vs. Production Stability**

**The Business Risk:** Fraud patterns evolve constantly, so our detection models needed
frequent updates. Shipping a new model straight into the live decision path was dangerous:
a bad model version could flood analysts with false positives, or worse, silently miss real
fraud until losses showed up weeks later.

**The Engineering Strategy:** I built a champion/challenger deployment pattern into the
pipeline. Every transaction was scored by the champion model for live decisions, while the
challenger (candidate) model scored the same events in shadow mode. Both score sets were
logged to Snowflake, letting us compare precision, recall, and false-positive rates on real
production traffic before promoting anything.

```java
// Champion scores the live decision path; challenger runs in shadow
ScoredTransaction result = transactions
    .keyBy(Transaction::getCustomerId)
    .process(new DualScoringFunction(championModel, challengerModel));

// Shadow scores are logged, never used for live decisions
result.getShadowOutput()
    .addSink(new SnowflakeModelComparisonSink());
```

Promotion was staged: shadow mode, then a small traffic slice (5%), then full rollout only
after the challenger beat the champion on agreed metrics. A versioned model registry let us
roll back in minutes if an approved model regressed in the wild.

**The Production Impact:** Model updates shipped weekly instead of quarterly, with zero
surprise regressions in production. Analysts trusted the alerts because every promotion
came with measured evidence on real traffic, not just offline test sets.

> 💡 **Key Takeaway:** "A new fraud model is a hypothesis, not a fact. We ran challengers in
> shadow on live traffic, promoted them in stages, and kept instant rollback, so model
> iteration stayed fast without risking the business."
