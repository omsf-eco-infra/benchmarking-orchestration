FROM --platform=linux/amd64 ghcr.io/prefix-dev/pixi:jammy-cuda-12.9.1 AS build

WORKDIR /app

# Copy project files and install the locked bench environment.
COPY . .
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && apt-get upgrade -y 
RUN apt-get install -y git
ENV CONDA_OVERRIDE_CUDA=12.0
RUN pixi install --locked -e bench

# Build a small entrypoint that activates the pixi env, then runs the command.
RUN pixi shell-hook -e bench -s bash > /shell-hook \
    && printf '#!/usr/bin/env bash\nset -euo pipefail\n' > /app/entrypoint.sh \
    && cat /shell-hook >> /app/entrypoint.sh \
    && printf '\nexec "$@"\n' >> /app/entrypoint.sh \
    && chmod 0755 /app/entrypoint.sh

# Clone the benchmarking repo
RUN git clone -b industry_benchmarks --single-branch https://github.com/OpenFreeEnergy/performance_benchmarks.git /app/performance_benchmarks

ENV NVIDIA_VISIBLE_DEVICES=all \
    NVIDIA_DRIVER_CAPABILITIES=compute,utility \
    PYTHONUNBUFFERED=1

# Keep the same /app prefix as build, because the package is installed editable.
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["python", "-m", "benchmarking_orchestration"]
