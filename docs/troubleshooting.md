# Troubleshooting

Find the message you see, then follow the fix. If none of these match, post in the course discussion board with
your operating system, the command you ran, and the full error message.

## Installing Docker

**"Virtualization: Disabled" in Task Manager, or Docker Desktop says virtualization isn't enabled (Windows)**
Turn on virtualization in your computer's BIOS/UEFI (often called *Intel VT-x*, *AMD-V*, or *SVM*). Restart,
press the setup key shown on screen (often `F2`, `F10`, `Del`, or `Esc`), enable it, save, and restart. Search
"enable virtualization" plus your laptop model for exact steps.

**"WSL 2 installation is incomplete" or "Docker Desktop requires a newer WSL kernel" (Windows)**
Open PowerShell as Administrator and run `wsl --update`, then restart your computer and start Docker Desktop again.

**"Enter new UNIX username" prompt, or "Catastrophic failure ... Wsl/Service/E_UNEXPECTED" (Windows)**
This happens when `wsl --install` is run without `--no-distribution` on a computer that already has WSL: Windows
installs Ubuntu as well and asks you to create a Linux user. Docker doesn't need Ubuntu.
- If the prompt accepts a username and password and finishes, that's fine: type `exit` and carry on with Lab 2.
- If it keeps repeating *Catastrophic failure* and asking for a username again, remove the half-installed Ubuntu:
  1. Press `Ctrl + C`, then close that window.
  2. Open PowerShell **as administrator** and run, one at a time:

     ```powershell
     wsl --shutdown
     wsl --update
     wsl --unregister Ubuntu
     wsl --list --verbose
     ```

     The last command should list only `docker-desktop`. **Never** unregister `docker-desktop`; Docker runs on it.
  3. Start Docker Desktop, wait for **Engine running**, and continue with Part C in a regular PowerShell window.

**I use VirtualBox (Windows)**
Recent VirtualBox versions run alongside Docker Desktop on WSL 2. Don't run VirtualBox machines while the labs are
running; they compete for memory.

## Running commands

**`docker: command not found` (macOS/Linux) or `'docker' is not recognized` (Windows)**
Docker isn't installed yet, or the terminal was opened before the install finished. Close the terminal and open a
new one. If it still happens, reinstall Docker Desktop.

**`Cannot connect to the Docker daemon` or `error during connect ... docker_engine`**
Docker Desktop isn't running. Start it and wait for **Engine running** before trying again.
On Linux, run `sudo systemctl start docker`.

**`permission denied while trying to connect to the Docker daemon socket` (Linux)**
Run `sudo usermod -aG docker $USER`, then log out and back in.

**`docker-compose: command not found`**
Use `docker compose` with a **space**. The older `docker-compose` command is not used in this course.

**`no configuration file provided: not found`**
You're not in the folder that holds `compose.yaml`. For Lab 2, `cd` into `labs/lab02` and run `ls` to check that
`compose.yaml` is listed.

**`Bind for 127.0.0.1:8080 failed: port is already allocated`**
Something else is using port 8080, often the `lab02-web` container from Part E1. Run `docker ps` and remove any
leftover container with `docker rm -f lab02-web`. If another program uses 8080, change `8080` to `8081` in the
command or in `compose.yaml` and open <http://localhost:8081> instead.

**`Error response from daemon: Conflict. The container name "/lab02-web" is already in use`**
A container with that name already exists. Remove it with `docker rm -f lab02-web` and run the command again.

**`docker run hello-world` hangs or says it can't pull the image**
Check your internet connection. On a campus or work network that blocks Docker Hub, try another network such as
home Wi-Fi.

**`toomanyrequests: You have reached your unauthenticated pull rate limit`**
Docker Hub limits downloads for people who aren't signed in, and a shared campus network can reach that limit
quickly. Create a free account at <https://hub.docker.com>, then sign in from Docker Desktop (top-right **Sign in**)
or run `docker login`. Wait a few minutes and run the command again.

**`ls` shows a table with no file names (Windows)**
The PowerShell window is too narrow, so it drops the **Name** column. Widen or maximize the window, or run
`ls -Name` to list just the names.

**I unzipped the files but can't find `labs` or `scripts`**
GitHub's ZIP creates a folder named `healthriskai-labs-main`, and Windows sometimes nests it inside another folder
of the same name. Open the folders until you see `docs`, `labs`, and `scripts` side by side; that's the repository
folder. Rename it to `healthriskai-labs` so the lab commands match.

## Running the environment check

**`running scripts is disabled on this system` (Windows)**
Use the exact command from Lab 2, which allows this one script to run without changing your settings:
`powershell -ExecutionPolicy Bypass -File scripts\check-docker.ps1 | Tee-Object lab02-check.txt`

**`No such file or directory: scripts/check-docker.sh`**
Run it from the repository folder (the one that contains `scripts`), not from `labs/lab02`. Run `cd ../..` first.

**WARN: memory available to Docker is low**
Close other apps (browsers use a lot). On macOS, raise it in Docker Desktop > Settings > Resources.
On Windows, Docker gets half of your RAM by default. To set it yourself, create a file named `.wslconfig` in your
user folder (`C:\Users\<your name>\.wslconfig`) containing:

```ini
[wsl2]
memory=10GB
```

Then run `wsl --shutdown` in PowerShell and start Docker Desktop again. Use `10GB` on a 16 GB computer, or `6GB` on
an 8 GB computer.

**WARN: free disk space is low**
Delete large files you no longer need or empty the trash. Docker images for the later labs need about 30 GB.

## Labs 3 and 4: Kafka and Spark

**`no configuration file provided: not found` when running `docker compose` in Lab 3 or 4**
From Lab 3 on, run every command from the repository folder (the one that contains `compose.yaml`), not from
`labs\lab03`. Run `cd $HOME\Documents\csc894\healthriskai-labs` and try again.

**`docker compose ps` shows kafka or spark-master as `(health: starting)` or `(unhealthy)`**
Give it a minute and run `docker compose ps` again. If it stays unhealthy, look at the log with
`docker compose logs kafka` (or `spark-master`). Restart everything with `docker compose down` and then
`docker compose up -d`.

**`Bind for 127.0.0.1:8081 failed: port is already allocated` (or 4040)**
Another program, or a leftover container, is using that port. Run `docker ps` and remove leftovers with
`docker rm -f NAME`. If it's another program, close it and run `docker compose up -d` again.

**`Put the IQVIA CSV file from Canvas in the data/iqvia folder`**
The scripts can't find the IQVIA file. Download it from Canvas, unzip it if needed, and put the `.csv` file in the
`data\iqvia` folder inside the repository folder (create the `iqvia` folder if it isn't there). Keep exactly one
`.csv` file in that folder. Never commit or share this file: it is licensed data.

**`TopicExistsException: Topic 'iqvia_nmta' already exists`**
You created the topic earlier. That's fine: continue with the next step.

**The Kafka connector download fails (`unresolved dependency` or a connection error mentioning `repo1.maven.org`)**
The first `--kafka` run downloads Spark's Kafka connector from the internet. Check your connection and run the
command again. Some campus or work networks block it; try another network such as home Wi-Fi.

**Lab 3, Part F: the streaming job never shows the 20 IQVIA rows**
The job only reads records that arrive *after* it starts. Wait until window 1 shows **Waiting for IQVIA records**,
then publish the sample file again from window 2.

**Lab 4: `No such file` for `data/raw/visits/visits_2025-12.jsonl`, or `No files match data/raw/visits/...`**
Build the patient visits first (Lab 4, Part B, step B3). It takes a few minutes.

**Lab 4: `No records found in Kafka topic patient_visits_lab4`**
The topic is empty. Do Part C (create the topic and publish the visits) before running the pipeline. Topics are
deleted by `docker compose down`, so after a restart you need to repeat Part C.

**Lab 4: the Kafka run shows more than 27,939 input records**
You published the visits more than once. The checks still pass (every resent visit is caught as a duplicate). To get
the standard numbers, delete the topic, then repeat Part C:
`docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic patient_visits_lab4`

**Lab 4: Check 3 is missing from the summary**
Check 3 compares the output with the original IQVIA file, so it only runs for the full run (`--source file`) and
only when the IQVIA file is in `data\iqvia`.

**A container stops with exit code 137, or Spark reports it is out of memory**
Docker doesn't have enough memory. Close other apps (browsers use a lot) and check the memory advice under
"Running the environment check" above. Labs 3 and 4 need about 4–5 GB for Docker. The full Lab 4 run is the largest job; if it fails, close other apps and run it again.

**Linux: you can't delete files in `data/curated` or `data/dlq`**
Spark runs as the root user so it can write on every operating system, which makes those files root-owned on
Linux. Delete them through Docker instead: `docker compose exec spark-master rm -rf data/curated data/dlq`.

## Labs 5 and 6: HDFS

**`no such service: namenode`, or `service "namenode" is not running`**
HDFS is in the Compose profile `hdfs`. Turn it on in the PowerShell window you're using, then run your command
again: `$env:COMPOSE_PROFILES = "hdfs"` (macOS / Linux: `export COMPOSE_PROFILES=hdfs`). Do this in every new
window, and before `docker compose down`, or the HDFS containers keep running.

**`docker compose ps` shows namenode or datanode as `(unhealthy)`, or they keep restarting**
Look at the log with `docker compose logs namenode` (or `datanode`). Most often Docker is short on memory: close
other apps, then run `docker compose down` and `docker compose up -d`. Labs 5 and 6 run five services and work best
with 16 GB of RAM.

**`Name node is in safe mode` when creating folders or uploading**
HDFS is still starting. Run `docker compose exec namenode hdfs dfsadmin -safemode wait`, then repeat the command.

**A Spark job says `Path does not exist: hdfs://namenode:8020/datalake/...`**
The raw zone is empty, usually because `docker compose down` removed the HDFS data. Load it again (Lab 5, Part C, or
Lab 6, Part B3). In Lab 6, run the days in order: Day 2 needs Day 1's output, and Day 5 needs Days 1 to 3.

**Mac with Apple silicon (M1 to M4): HDFS is very slow or won't start**
The Hadoop image is built for Intel/AMD processors, so Docker Desktop runs it through emulation. In Docker Desktop,
open **Settings** > **General** and turn on **Use Rosetta for x86_64/amd64 emulation on Apple Silicon**, then
restart Docker Desktop.

**`Bind for 127.0.0.1:9870 failed: port is already allocated`**
Another program or a leftover container is using the HDFS web port. Run `docker ps`, remove leftovers with
`docker rm -f NAME`, and run `docker compose up -d` again.
