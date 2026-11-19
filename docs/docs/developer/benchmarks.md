---
sidebar_position: 4
---

# Benchmarks

The repository has a benchmark that compares Raio with other middleware.

## Setup

The benchmark measures **latency**: the time to send an array from one node to another and multiply it, for a range of array sizes. It compares rclpy (ROS 2), Ray, Rosia, and Raio. The setup is that of the [Rosia benchmark](https://github.com/rosia-project/rosia-py/tree/main/benchmark).

## Running

The benchmark runs in Docker:

```bash
docker compose -f benchmark/docker-compose.yml up --build
```

Results are saved to `benchmark/results/`.

## Results

Results are not published here yet.
