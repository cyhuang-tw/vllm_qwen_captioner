# Qwen3-Captioner + vLLM

## Manual Setup
### Start the Server
```bash
./start_server.sh
```

### Start the Client
On the same node, run:
```bash
./start_client.sh
```
If you are using arkive format, run:
```bash
./start_client_arkive.sh
```

## Automatic Split SCP/ARKIVE into Jobs
For SCP file:
```bash
bash batch_submit.sh
```

For ARKIVE file:
```bash
bash batch_submit_arkive.sh
```
