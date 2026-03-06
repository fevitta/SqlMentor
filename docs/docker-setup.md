# Docker Setup para Testes de Integracao (WSL2 Ubuntu)

## Instalacao do Docker Engine no WSL2

Existem duas abordagens: **Docker Engine direto no WSL2** (recomendado, leve) ou Docker Desktop no Windows. Este guia cobre a primeira opcao.

### 1. Remover versoes antigas (se houver)

```bash
sudo apt remove docker docker-engine docker.io containerd runc 2>/dev/null
```

### 2. Instalar dependencias e adicionar repositorio oficial

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
```

### 3. Instalar Docker Engine + Compose plugin

```bash
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

### 4. Permitir uso sem sudo

```bash
sudo usermod -aG docker $USER
```

**Importante**: Feche e reabra o terminal WSL (ou execute `newgrp docker`) para o grupo ter efeito.

### 5. Iniciar o servico Docker

No WSL2 sem systemd, o Docker precisa ser iniciado manualmente:

```bash
sudo service docker start
```

Para verificar:

```bash
docker info
```

Se `sudo service docker start` nao funcionar, tente:

```bash
sudo dockerd &
```

### 6. (Opcional) Iniciar Docker automaticamente ao abrir WSL

Adicione ao seu `~/.bashrc` ou `~/.profile`:

```bash
# Auto-start Docker daemon no WSL2
if ! pgrep -x dockerd > /dev/null; then
    sudo service docker start > /dev/null 2>&1
fi
```

Para evitar que peca senha, adicione ao sudoers:

```bash
sudo visudo
# Adicione no final:
# <seu_usuario> ALL=(ALL) NOPASSWD: /usr/sbin/service docker *
```

### 7. Verificar instalacao

```bash
docker --version
docker compose version
docker run --rm hello-world
```

---

## Subindo o Oracle para Testes

### Primeira execucao (download da imagem + init scripts)

```bash
cd /mnt/d/Code/Github/SqlMentor
docker compose up -d
```

A primeira execucao demora mais (~2-5 min) porque:
1. Baixa a imagem `gvenzl/oracle-xe:21-slim` (~2GB)
2. Inicializa o banco Oracle
3. Executa os scripts em `tests/integration/initdb/` (cria user, schema, dados)

### Acompanhar inicializacao

```bash
# Ver logs em tempo real
docker compose logs -f oracle

# Verificar health status
docker inspect --format='{{.State.Health.Status}}' sqlmentor-oracle
```

Quando o status mudar para `healthy`, o banco esta pronto.

### Rodar testes de integracao

```bash
# Todos os testes de integracao
task test-oracle

# Ou diretamente
pytest tests/integration/ -v --tb=short

# Apenas um arquivo
pytest tests/integration/test_connection.py -v

# Testes unitarios + integracao juntos
task test-all
```

### Parar e limpar

```bash
# Parar container (mantem imagem para proxima vez)
docker compose down

# Parar e remover volumes (reset completo do banco)
docker compose down -v

# Remover imagem (libera ~2GB)
docker rmi gvenzl/oracle-xe:21-slim
```

### Re-execucoes subsequentes

Apos a primeira vez, `docker compose up -d` roda em ~30s (imagem ja baixada, mas os init scripts rodam novamente pois usamos `tmpfs`).

---

## Troubleshooting

### "Cannot connect to the Docker daemon"

O servico Docker nao esta rodando:

```bash
sudo service docker start
```

### "Permission denied" ao rodar docker

Seu usuario nao esta no grupo `docker`:

```bash
sudo usermod -aG docker $USER
newgrp docker   # Aplica sem relogar
```

### Container nao fica healthy

Verifique os logs:

```bash
docker compose logs oracle | tail -30
```

Causas comuns:
- **Memoria insuficiente**: Oracle XE precisa de ~2GB RAM livre. Verifique com `free -h`.
- **Porta 1521 em uso**: Outro processo usando a porta. Mude com `ORACLE_PORT=1522 docker compose up -d`.

### Testes pulados com "Oracle nao disponivel"

O container nao esta rodando ou os init scripts falharam:

```bash
# Verificar container
docker ps

# Testar conexao manual
python3 -c "
import oracledb
conn = oracledb.connect(user='SQLMENTOR_TEST', password='TestPwd123',
    dsn=oracledb.makedsn('localhost', 1521, service_name='XEPDB1'))
print('OK:', conn.version)
conn.close()
"
```

### WSL2 com pouca memoria

Crie/edite `%USERPROFILE%\.wslconfig` no Windows:

```ini
[wsl2]
memory=4GB
swap=2GB
```

Reinicie WSL: `wsl --shutdown` no PowerShell, depois reabra.
