# Oracle Instant Client — Ubuntu (WSL2)

Necessário apenas para Oracle < 12c (modo thick).

## 1. Dependência do sistema

```bash
sudo apt update
sudo apt install libaio1t64 unzip wget
sudo ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1
```

> No Ubuntu mais antigo o pacote é `libaio1` (sem o sufixo `t64`).

## 2. Baixar e instalar o Instant Client

```bash
cd /tmp
wget https://download.oracle.com/otn_software/linux/instantclient/2326100/instantclient-basic-linux.x64-23.26.1.0.0.zip
sudo mkdir -p /opt/oracle
sudo unzip instantclient-basic-linux.x64-23.26.1.0.0.zip -d /opt/oracle
```

Se o link estiver desatualizado, baixe o **Basic Package (ZIP)** direto de:
https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html

## 3. Configurar LD_LIBRARY_PATH

Verifique o nome exato da pasta:

```bash
ls /opt/oracle/
# ex: instantclient_23_26
```

Adicione ao `.bashrc`:

```bash
echo 'export LD_LIBRARY_PATH=/opt/oracle/instantclient_23_26:$LD_LIBRARY_PATH' >> ~/.bashrc
source ~/.bashrc
```

## 4. Validar

```bash
cd /mnt/d/Code/Github/SqlMentor
source .venv/bin/activate
sqlmentor doctor
```

O check de thick mode deve mostrar `✓ Disponível`.

```bash
sqlmentor config add --name dev --user sqlmentor --host 192.168.1.100 --port 1521 --service ORCL --schema sqlmentor --password mysecret

sqlmentor config test -n dev

sqlmentor config set-default -n dev
```
