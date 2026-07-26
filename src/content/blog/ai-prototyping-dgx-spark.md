---
title: "NVIDIA DGX Spark Arrived!"
description: "I have never been this excited about a computer arriving. NVIDIA DGX Spark is the machine I want to use for local LLM inference, GPU systems learning, benchmarking, RAG experiments, ETL, fine tuning, and local AI infrastructure work."
pubDate: "Oct 17 2025"
heroImage: "/dgx-spark-unboxing.jpg"
---

I do not remember the last time I was this excited about a computer showing up.

The NVIDIA DGX Spark arrived, and it feels less like unboxing another workstation and more like putting a small AI lab on the desk. I have spent years around software systems, cloud infrastructure, distributed services, and the usual developer machines. This one feels different because it is not just faster hardware. It is an invitation to get closer to the full stack of modern AI systems.

The first thing I want from it is simple: serious local LLM inference. I want to run models locally, measure them properly, understand where latency comes from, and see what changes when the machine is built for this class of work instead of being forced into it.

But the bigger plan is to learn the system underneath the demos.

I want to use DGX Spark to study NVIDIA's production stacks, GPU internals, and the infrastructure engineering patterns around AI workloads. That means getting hands-on with CUDA behavior, memory pressure, batching, model serving, observability, and the less glamorous parts that decide whether an AI system feels magical or merely expensive.

## What I Want To Build And Learn

The roadmap is a mix of curiosity and practical systems work:

- **Local LLM inference:** run useful models close to the developer loop and understand the real tradeoffs
- **Benchmarking:** measure throughput, latency, batching behavior, and sustained load instead of guessing
- **RAG experiments:** build retrieval pipelines that can be tested locally with realistic embeddings and model calls
- **ETL for AI systems:** process, structure, embed, and evaluate data as part of a repeatable pipeline
- **Fine tuning:** explore where model adaptation makes sense and what the workflow actually costs
- **GPU internals:** learn how the hardware behaves under pressure, not just what the spec sheet says
- **Infrastructure system engineering:** treat local AI as a full system with deployment, monitoring, reliability, and iteration loops

## Why This Matters

Cloud AI is incredibly useful, but it can hide too much of the machinery. A local AI machine gives me a place to slow down, instrument things, break them, fix them, and understand the stack from the model call down to the GPU.

That is what makes the DGX Spark exciting to me. It is not just a box with an NVIDIA logo on it. It is a way to make local AI feel real enough for experimentation, deep enough for systems learning, and practical enough to influence how I build production workflows.

I am going to use it to learn in public, test assumptions, and build a sharper intuition for the infrastructure behind modern AI.
