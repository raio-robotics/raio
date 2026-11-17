# Benchmark

Compares rclpy (ROS 2) vs Ray vs rosia vs raio for array multiplication latency across
different array sizes.

The setup is that of the
[rosia benchmark](https://github.com/rosia-project/rosia-py/tree/main/benchmark), and
`rclpy_bench.py`, `ray_bench.py` and `rosia_bench.py` are its files unchanged.

## Run

```bash
docker compose -f benchmark/docker-compose.yml up --build
```

Results are saved to `benchmark/results/`.
