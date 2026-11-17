FROM ros:humble

RUN apt-get update && apt-get install -y \
    python3-pip \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && echo "source /opt/ros/humble/setup.bash" >> ~/.bashrc

RUN pip install --upgrade pip setuptools && \
    pip install numpy ray[default] matplotlib rosia

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

COPY . /raio-src
RUN pip install /raio-src/raio-py

WORKDIR /benchmark

COPY benchmark/benchmarks/ ./benchmarks/

CMD ["bash", "-c", "source /opt/ros/humble/setup.bash && cd benchmarks && python3 run_benchmarks.py"]
