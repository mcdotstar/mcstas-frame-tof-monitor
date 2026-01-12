# Frame_monitor

Mimic a histogram intensity versus time monitor reset by, e.g., a source TTL signal.

## Testing

### Integration Tests with Kafka

The test suite includes integration tests for the `Frame_monitor_json` component that sends data to Kafka. These tests use [testcontainers](https://testcontainers-python.readthedocs.io/) to run a lightweight Redpanda broker (Kafka-compatible).

#### Local Development (Podman)

If you have Podman installed, testcontainers will automatically use it. You need to:

1. Enable the Podman socket:

```bash
systemctl --user enable --now podman.socket
```

2. Set the `DOCKER_HOST` environment variable:

```bash
export DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock
```

Then run your tests as normal. The Ryuk reaper container is automatically disabled for Podman compatibility.

#### CI/Remote Testing (Docker)

GitHub Actions' `ubuntu-latest` runners have Docker pre-installed and available, so no additional setup is needed.


